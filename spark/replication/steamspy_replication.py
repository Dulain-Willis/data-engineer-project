"""Replicates iceberg.steamspy.silver (silver) → ClickHouse analytics.steamspy_silver.

Change detection uses Iceberg snapshot IDs stored in analytics.replication_state.
Because silver.py uses .overwritePartitions() (OVERWRITE snapshots), the standard
Iceberg incremental scan API (APPEND-only) cannot be used. Instead, this job queries
the `entries` metadata table to find which dt partitions were written in new snapshots,
then re-reads those partitions from the current snapshot and reloads them in ClickHouse.

Spark conf keys (set by Airflow DAG params):
    spark.replication.from_snapshot  — override start snapshot ID (exclusive)
    spark.replication.full_load      — "true" to ignore state and reload everything
"""
import os

from pyspark import AccumulatorParam
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipelines.common.clickhouse.client import get_client
from pipelines.common.spark.session import build_spark_session
from pipelines.replication.state import get_last_clickhouse_snapshot_id, update_snapshot_id

ICEBERG_TABLE = "iceberg.steamspy.silver"
CLICKHOUSE_TABLE = "analytics.steamspy_silver"
STATE_KEY = ICEBERG_TABLE

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "clickhouse")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "analytics")


class _IntegerAccumulator(AccumulatorParam):
    """Spark AccumulatorParam for summing integer counts across executor partitions."""

    def zero(self, value):
        """Return the zero value for integer accumulation."""
        return 0

    def addInPlace(self, v1, v2):
        """Add two integer accumulator values in place."""
        return v1 + v2


def get_last_iceberg_snapshot_id(spark: SparkSession) -> int:
    """Return the snapshot ID of the most recently committed Iceberg snapshot.

    Args:
        spark: A SparkSession configured with the Iceberg catalog for ICEBERG_TABLE.

    Returns:
        The integer snapshot ID of the latest committed snapshot.

    Raises:
        RuntimeError: When no snapshots exist for ICEBERG_TABLE, indicating silver
            has not run yet.
    """
    snapshot_rows = spark.sql(f"""
        SELECT snapshot_id
        FROM {ICEBERG_TABLE}.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
    """
    ).collect()

    if not snapshot_rows:
        raise RuntimeError(f"No snapshots found for {ICEBERG_TABLE} — has silver run yet?")

    return int(snapshot_rows[0][0])


def get_unreplicated_partition_dates(spark: SparkSession, last_replicated_snapshot_id: int) -> list[str]:
    """Return date partition values written in snapshots committed after last_clickhouse_snapshot_id.

    Queries the Iceberg `entries` metadata table (status=1 means file was added in that
    snapshot). Works correctly for both APPEND and OVERWRITE operations, unlike the
    standard incremental scan API which only supports APPEND.

    Args:
        spark: A SparkSession configured with the Iceberg catalog for ICEBERG_TABLE.
        last_clickhouse_snapshot_id: The snapshot ID of the last successfully replicated
            snapshot. Only snapshots committed after this ID are inspected.

    Returns:
        A list of dt partition string values (e.g. ["2025-01-15", "2025-01-16"]) that
        were written in snapshots newer than last_clickhouse_snapshot_id. Returns an
        empty list if no new partitions were written.
    """
    partition_rows = spark.sql(f"""
        SELECT DISTINCT partition.dt
        FROM {ICEBERG_TABLE}.entries
        WHERE snapshot_id IN (
            SELECT snapshot_id
            FROM {ICEBERG_TABLE}.snapshots
            WHERE snapshot_id > {last_replicated_snapshot_id}
        )
        AND status = 1
    """).collect()
    return [row[0] for row in partition_rows if row[0] is not None]


def get_all_iceberg_partition_dates(spark: SparkSession) -> list[str]:
    """Return all distinct dt partition values present in the Iceberg silver table.

    Used in two cases where there is no delta to compute from:
      - First run: no prior replication state exists, so there is no baseline snapshot
        ID to compare against and all partitions must be loaded.
      - Full load override: caller explicitly wants everything reloaded regardless of state.

    Args:
        spark: A SparkSession configured with the Iceberg catalog for ICEBERG_TABLE.

    Returns:
        A list of all dt partition string values currently in ICEBERG_TABLE.
    """
    partition_rows = spark.sql(f"""
        SELECT DISTINCT dt 
        FROM {ICEBERG_TABLE}
    """).collect()
    return [row[0] for row in partition_rows if row[0] is not None]


def delete_clickhouse_date_partitions(date_partitions: list[str]) -> None:
    """Clear the given dt partitions from ClickHouse before reload for idempotency.

    Deletes all rows where the value in the dt column matches any of the dates about 
    to be replicated so that a subsequent insert produces a clean, deduplicated result.

    Args:
        date_partitions: List of dt string values whose rows should be deleted from
            CLICKHOUSE_TABLE (e.g. ["2025-01-15", "2025-01-16"]).
    """
    clickhouse_client = get_client()

    for date_partition in date_partitions:
        clickhouse_client.execute(
            f"ALTER TABLE {CLICKHOUSE_TABLE} DELETE WHERE dt = %(dt)s",
            {"dt": date_partition},
        )

        print(f"  Cleared ClickHouse partition dt={date_partition}")


def write_dataframe_to_clickhouse(spark: SparkSession, dataframe) -> int:
    """Write a Spark DataFrame to ClickHouse using foreachPartition and clickhouse-driver.

    Uses a Spark accumulator to track the total row count across all executor partitions,
    since return values from foreachPartition are not directly accessible on the driver.

    Args:
        spark: A SparkSession used to create the accumulator for row counting.
        dataframe: The Spark DataFrame to write. Each row must be compatible with the
            column schema of CLICKHOUSE_TABLE.

    Returns:
        The total number of rows written to CLICKHOUSE_TABLE across all partitions.
    """
    host = CLICKHOUSE_HOST
    port = CLICKHOUSE_PORT
    user = CLICKHOUSE_USER
    password = CLICKHOUSE_PASSWORD
    database = CLICKHOUSE_DATABASE
    table = CLICKHOUSE_TABLE

    rows_written_accumulator = spark.sparkContext.accumulator(0, _IntegerAccumulator())

    def write_partition(partition_rows):
        from clickhouse_driver import Client

        clickhouse_client = Client(host=host, port=port, user=user, password=password, database=database)
        rows_batch = [row.asDict() for row in partition_rows]
        if rows_batch:
            clickhouse_client.execute(f"INSERT INTO {table} VALUES", rows_batch)
            rows_written_accumulator.add(len(rows_batch))

    dataframe.foreachPartition(write_partition)
    return rows_written_accumulator.value


def main():
    """Detect changed Iceberg partitions and replicate them to ClickHouse.

    Reads replication state from ClickHouse to determine which Iceberg snapshots have
    already been replicated, identifies dt partitions written in newer snapshots, clears
    those partitions in ClickHouse, reloads them from the current Iceberg snapshot, and
    updates the stored snapshot ID upon success.
    """
    spark = build_spark_session("steamspy-games-replication")
    clickhouse_client = get_client()

    try:
        airflow_param_passed_snapshot_id = spark.conf.get("spark.replication.from_snapshot", "")
        airflow_param_passed_is_full_load = spark.conf.get("spark.replication.full_load", "false").lower() == "true"

        last_iceberg_snapshot_id = get_last_iceberg_snapshot_id(spark)
        print(f"Current Iceberg snapshot ID: {last_iceberg_snapshot_id}")

        if airflow_param_passed_is_full_load:
            print("full_load=true: reloading all partitions")
            affected_date_partitions = get_all_iceberg_partition_dates(spark)

        else:
            override_snapshot_id = int(airflow_param_passed_snapshot_id) if airflow_param_passed_snapshot_id else None

            last_clickhouse_snapshot_id = override_snapshot_id or get_last_clickhouse_snapshot_id(clickhouse_client, STATE_KEY)
            print(f"Last replicated snapshot ID: {last_clickhouse_snapshot_id}")

            if last_clickhouse_snapshot_id is not None and last_clickhouse_snapshot_id == last_iceberg_snapshot_id:
                print("Already up to date — nothing to replicate")
                return

            if last_clickhouse_snapshot_id is None:
                print("No previous state — performing full load")
                affected_date_partitions = get_all_iceberg_partition_dates(spark)
            else:
                print(f"Detecting partitions changed since snapshot {last_clickhouse_snapshot_id}")
                affected_date_partitions = get_unreplicated_partition_dates(spark, last_clickhouse_snapshot_id)

        if not affected_date_partitions:
            print("No affected partitions detected — nothing to replicate")
            return

        print(f"Affected dt partitions: {affected_date_partitions}")

        delete_clickhouse_date_partitions(affected_date_partitions)

        affected_partitions_df = spark.table(ICEBERG_TABLE).filter(col("dt").isin(affected_date_partitions))

        rows_written = write_dataframe_to_clickhouse(spark, affected_partitions_df)
        print(f"Wrote {rows_written} rows to {CLICKHOUSE_TABLE}")

        update_snapshot_id(clickhouse_client, STATE_KEY, get_last_iceberg_snapshot_id(spark))
        print(f"State updated → snapshot_id={get_last_iceberg_snapshot_id(spark)}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

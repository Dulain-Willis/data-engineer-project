"""
Replicates iceberg.steamspy.games (silver) → ClickHouse analytics.steamspy_silver.

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
from pipelines.replication.state import get_last_snapshot_id, update_snapshot_id

ICEBERG_TABLE = "iceberg.steamspy.games"
CLICKHOUSE_TABLE = "analytics.steamspy_silver"
STATE_KEY = ICEBERG_TABLE

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "clickhouse")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "analytics")


class _IntAccumulator(AccumulatorParam):
    def zero(self, value):
        return 0

    def addInPlace(self, v1, v2):
        return v1 + v2


def get_current_snapshot_id(spark: SparkSession) -> int:
    rows = spark.sql(
        f"SELECT snapshot_id FROM {ICEBERG_TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not rows:
        raise RuntimeError(f"No snapshots found for {ICEBERG_TABLE} — has silver run yet?")
    return int(rows[0][0])


def get_affected_dts(spark: SparkSession, last_snapshot_id: int) -> list[str]:
    """
    Returns dt partition values written in any snapshot committed after last_snapshot_id.

    Queries the Iceberg `entries` metadata table (status=1 means file was added in that
    snapshot). Works correctly for both APPEND and OVERWRITE operations, unlike the
    standard incremental scan API which only supports APPEND.
    """
    rows = spark.sql(f"""
        SELECT DISTINCT partition.dt
        FROM {ICEBERG_TABLE}.entries
        WHERE snapshot_id IN (
            SELECT snapshot_id
            FROM {ICEBERG_TABLE}.snapshots
            WHERE snapshot_id > {last_snapshot_id}
        )
        AND status = 1
    """).collect()
    return [row[0] for row in rows if row[0] is not None]


def get_all_dts(spark: SparkSession) -> list[str]:
    rows = spark.sql(f"SELECT DISTINCT dt FROM {ICEBERG_TABLE}").collect()
    return [row[0] for row in rows if row[0] is not None]


def delete_ch_partitions(dts: list[str]) -> None:
    """Clears affected dt partitions in ClickHouse before reload for idempotency."""
    client = get_client()
    for dt in dts:
        client.execute(
            f"ALTER TABLE {CLICKHOUSE_TABLE} DELETE WHERE dt = %(dt)s",
            {"dt": dt},
        )
        print(f"  Cleared ClickHouse partition dt={dt}")


def write_to_clickhouse(spark: SparkSession, df) -> int:
    """
    Writes a Spark DataFrame to ClickHouse using foreachPartition + clickhouse-driver.
    Returns the total row count written (tracked via Spark accumulator).
    """
    host = CLICKHOUSE_HOST
    port = CLICKHOUSE_PORT
    user = CLICKHOUSE_USER
    password = CLICKHOUSE_PASSWORD
    database = CLICKHOUSE_DATABASE
    table = CLICKHOUSE_TABLE

    counter = spark.sparkContext.accumulator(0, _IntAccumulator())

    def write_partition(rows):
        from clickhouse_driver import Client

        ch = Client(host=host, port=port, user=user, password=password, database=database)
        batch = [row.asDict() for row in rows]
        if batch:
            ch.execute(f"INSERT INTO {table} VALUES", batch)
            counter.add(len(batch))

    df.foreachPartition(write_partition)
    return counter.value


def main():
    spark = build_spark_session("steamspy-games-replication")
    ch_client = get_client()

    try:
        from_snapshot_raw = spark.conf.get("spark.replication.from_snapshot", "")
        full_load = spark.conf.get("spark.replication.full_load", "false").lower() == "true"

        current_snapshot = get_current_snapshot_id(spark)
        print(f"Current Iceberg snapshot ID: {current_snapshot}")

        if full_load:
            print("full_load=true: reloading all partitions")
            affected_dts = get_all_dts(spark)
        else:
            override_snapshot = int(from_snapshot_raw) if from_snapshot_raw else None
            last_snapshot = override_snapshot or get_last_snapshot_id(ch_client, STATE_KEY)
            print(f"Last replicated snapshot ID: {last_snapshot}")

            if last_snapshot is not None and last_snapshot == current_snapshot:
                print("Already up to date — nothing to replicate")
                return

            if last_snapshot is None:
                print("No previous state — performing full load")
                affected_dts = get_all_dts(spark)
            else:
                print(f"Detecting partitions changed since snapshot {last_snapshot}")
                affected_dts = get_affected_dts(spark, last_snapshot)

        if not affected_dts:
            print("No affected partitions detected — nothing to replicate")
            update_snapshot_id(ch_client, STATE_KEY, current_snapshot)
            return

        print(f"Affected dt partitions: {affected_dts}")

        delete_ch_partitions(affected_dts)

        df = spark.table(ICEBERG_TABLE).filter(col("dt").isin(affected_dts))
        rows_written = write_to_clickhouse(spark, df)
        print(f"Wrote {rows_written} rows to {CLICKHOUSE_TABLE}")

        update_snapshot_id(ch_client, STATE_KEY, current_snapshot)
        print(f"State updated → snapshot_id={current_snapshot}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

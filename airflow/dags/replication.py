"""
Replication DAG: Iceberg silver tables → ClickHouse.

Runs independently from the steamspy extraction DAG. Trigger this after the
steamspy DAG completes, or manually for backfills. Snapshot-based change
detection ensures only modified partitions are reloaded.
"""
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from pipelines.common.spark.config import get_iceberg_catalog_conf, get_s3a_conf, get_spark_resource_conf


with DAG(
    dag_id="replication",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["replication", "clickhouse", "iceberg"],
    params={
        "from_snapshot": Param(
            None,
            type=["null", "integer"],
            description="Override start snapshot ID (exclusive). Leave null to use stored state.",
        ),
        "full_load": Param(
            False,
            type="boolean",
            description="Ignore state table and reload all partitions from scratch.",
        ),
    },
) as dag:

    replicate_steamspy = SparkSubmitOperator(
        task_id="replicate_steamspy",
        application="/opt/spark/jobs/replication/steamspy_replication.py",
        conn_id="spark_default",
        conf={
            **get_s3a_conf(),
            **get_spark_resource_conf(),
            **get_iceberg_catalog_conf(),
            # Enable Iceberg SQL extensions so metadata tables (entries, snapshots) are queryable
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            # Replication control params — read via spark.conf.get() in the Spark job
            "spark.replication.from_snapshot": "{{ params.from_snapshot or '' }}",
            "spark.replication.full_load": "{{ params.full_load | lower }}",
        },
    )

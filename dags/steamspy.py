"""Airflow DAG to land SteamSpy payloads and trigger PySpark refinement."""

from datetime import datetime
import json
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from src.extr.steamspy_api import call_steamspy_api
from src.load.minio_loader import upload_to_minio


bucket_name = "steam"
dataset_prefix = "steamspy"
bronze_prefix = f"bronze/{dataset_prefix}"
object_name = f"{bronze_prefix}/steamspy-payload.json"
spark_runner = Path(__file__).resolve().parents[1] / "spark" / "jobs" / "run_job.py"


@dag(
    dag_id="steamspy",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
)
def steamspy():

    @task
    def extract():
        return call_steamspy_api()

    @task
    def load(extract_output):
        json_to_bytes = json.dumps(extract_output).encode("utf-8")
        upload_to_minio(
            bucket_name,
            object_name,
            json_to_bytes,
            "application/json",
        )

    refine = SparkSubmitOperator(
        task_id="steamspy_silver_refine",
        application=str(spark_runner),
        conn_id="spark_default",
        name="steamspy-silver",
        application_args=["--job", "steamspy"],
        verbose=True,
    )

    raw_payload = extract()
    loaded = load(raw_payload)
    loaded >> refine


dag = steamspy()

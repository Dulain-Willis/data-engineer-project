from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import json

from src.extr.steamspy_api import call_steamspy_api
from src.load.minio_loader import upload_to_minio
from src.tran.spark.spark_conf import get_s3a_conf, get_spark_resource_conf

bucket_name = 'steamspy-dev-raw'
object_name = 'steamspy_json'

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
        upload_to_minio(bucket_name, object_name, json_to_bytes, "application/json")

    transform = SparkSubmitOperator(
        task_id="transform",
        application="/opt/airflow/dags/src/tran/spark/bronze_to_silver.py",
        conn_id="spark_default",
        conf={**get_s3a_conf(), **get_spark_resource_conf()},
    )

    # Pipeline: extract >> load >> transform
    load(extract()) >> transform

dag = steamspy()

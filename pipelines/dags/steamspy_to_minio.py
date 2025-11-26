from airflow.decorators import dag, task

from datetime import datetime
import json

from extr.steamspy_api import call_steamspy_api
from load.minio_loader import upload_to_minio

bucket_name = 'steamspy-dev-raw'
object_name = 'steamspy_json'

@dag(
    dag_id="steamspy_to_minio",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
)
def steamspy_to_minio():

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
            "application/json"
    )

    load(extract())

dag = steamspy_to_minio()

from airflow.decorators import dag 
from airflow.decorators import task 

from pendelum import datetime

@dag(
    dag_id="steamspy_to_minio",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)

def steamspy_to_minio_dag():

    @task
    def extract():
        data = get_steamspy_api() 
        return data 

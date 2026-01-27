from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

from src.extr.steamspy_api import call_steamspy_api
from src.tran.spark.spark_conf import get_s3a_conf, get_spark_resource_conf

bucket_name = 'bronze'

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
        ctx = get_current_context()
        run_id = ctx["run_id"]
        pages_uploaded = call_steamspy_api(bucket=bucket_name, run_id=run_id)
        return {"run_id": run_id, "pages_uploaded": pages_uploaded}

    # Get run_id from extract task for Spark jobs
    extract_task = extract()
    
    bronze = SparkSubmitOperator(
        task_id="bronze",
        application="/opt/airflow/dags/src/tran/spark/jobs/steamspy_bronze.py",
        conn_id="spark_default",
        conf={
            **get_s3a_conf(),
            **get_spark_resource_conf(),
            "spark.steamspy.run_id": "{{ run_id }}",
        },
    )
    
    silver = SparkSubmitOperator(
        task_id="silver",
        application="/opt/airflow/dags/src/tran/spark/jobs/steamspy_silver.py",
        conn_id="spark_default",
        conf={
            **get_s3a_conf(),
            **get_spark_resource_conf(),
            "spark.steamspy.run_id": "{{ run_id }}",
        },
    )

    # Pipeline: extract >> bronze >> silver
    extract_task >> bronze >> silver

dag = steamspy()

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context, ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

from pipelines.steamspy.extract import call_steamspy_api
from pipelines.common.spark.config import get_s3a_conf, get_spark_resource_conf, get_iceberg_catalog_conf
from pipelines.common.clickhouse.client import get_client
from pipelines.steamspy.load import load_partition

bucket_name = 'bronze'


@dag(
    dag_id="steamspy",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "force_refresh": False,  # Default: skip extraction, reuse bronze data
    },
)
def steamspy():

    # Guard logic: check if extraction should run
    def check_should_extract(**context) -> bool:
        # Returns True if extraction should run (force_refresh=True).
        # Returns False to skip extraction and re-use existing bronze data.
        force_refresh = context["params"].get("force_refresh", False)
        print(f"force_refresh parameter: {force_refresh}")

        if force_refresh:
            print("Extraction will run (force_refresh=True)")
        else:
            print("Skipping extraction (force_refresh=False). Re-using existing bronze data.")

        return force_refresh

    should_extract = ShortCircuitOperator(
        task_id="should_extract",
        python_callable=check_should_extract,
    )

    @task
    def extract():
        ctx = get_current_context()
        ds = ctx["ds"]
        run_id = ctx["run_id"]
        pages_uploaded = call_steamspy_api(bucket=bucket_name, ds=ds, run_id=run_id)
        return {"ds": ds, "run_id": run_id, "pages_uploaded": pages_uploaded}

    # Get run_id from extract task for Spark jobs
    extract_task = extract()

    bronze = SparkSubmitOperator(
        task_id="bronze",
        application="/opt/spark/jobs/steamspy/bronze.py",
        conn_id="spark_default",
        conf={
            **get_s3a_conf(),
            **get_spark_resource_conf(),
            "spark.steamspy.ds": "{{ ds }}",
            "spark.steamspy.run_id": "{{ run_id }}",
        },
    )

    silver = SparkSubmitOperator(
        task_id="silver",
        application="/opt/spark/jobs/steamspy/silver.py",
        conn_id="spark_default",
        conf={
            **get_s3a_conf(),
            **get_spark_resource_conf(),
            **get_iceberg_catalog_conf(),
            "spark.steamspy.ds": "{{ ds }}",
            "spark.steamspy.run_id": "{{ run_id }}",
        },
    )

    @task
    def load_clickhouse():
        ctx = get_current_context()
        ds = ctx["ds"]
        client = get_client()
        s3_path = f"silver/steamspy/dt={ds}"
        rows_loaded = load_partition(client, "steamspy_silver", s3_path, ds)
        return {"ds": ds, "rows_loaded": rows_loaded}

    load_ch = load_clickhouse()

    # Pipeline: should_extract >> extract >> bronze >> silver >> load_clickhouse
    should_extract >> extract_task >> bronze >> silver >> load_ch


dag = steamspy()

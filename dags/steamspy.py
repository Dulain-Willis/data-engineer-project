from airflow.decorators import dag, task
from airflow.operators.python import get_current_context, ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

from pipelines.steamspy.extract import call_steamspy_api
from pipelines.common.spark.config import get_s3a_conf, get_spark_resource_conf, get_iceberg_catalog_conf
from pipelines.common.clickhouse.client import get_client
from pipelines.steamspy.load import load_partition

bucket_name = 'landing'


@dag(
    dag_id="steamspy",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "force_refresh": False,  # Default: skip extraction, reuse existing Iceberg data
    },
)
def steamspy():

    def check_should_extract(**context) -> bool:
        force_refresh = context["params"].get("force_refresh", False)
        print(f"force_refresh parameter: {force_refresh}")

        if force_refresh:
            print("Extraction will run (force_refresh=True)")
        else:
            print("Skipping extraction (force_refresh=False). Re-using existing Iceberg data.")

        return force_refresh

    should_extract = ShortCircuitOperator(
        task_id="should_extract",
        python_callable=check_should_extract,
        ignore_downstream_trigger_rules=False,
    )

    @task
    def extract():
        """Extract data from SteamSpy API and upload to landing zone."""
        ctx = get_current_context()
        ds = ctx["ds"]
        run_id = ctx["run_id"]

        pages_uploaded = call_steamspy_api(bucket=bucket_name, ds=ds, run_id=run_id)

        return {"ds": ds, "run_id": run_id, "pages_uploaded": pages_uploaded}

    extract_task = extract()

    bronze = SparkSubmitOperator(
        task_id="bronze",
        application="/opt/spark/jobs/steamspy/bronze.py",
        conn_id="spark_default",
        conf={
            **get_s3a_conf(),
            **get_spark_resource_conf(),
            **get_iceberg_catalog_conf(),
            "spark.steamspy.ds": "{{ ds }}",
            "spark.steamspy.run_id": "{{ run_id }}",
        },
        trigger_rule="none_failed",
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
        trigger_rule="none_failed_min_one_success",
    )

    @task(trigger_rule="none_failed_min_one_success")
    def load_clickhouse():
        ctx = get_current_context()
        ds = ctx["ds"]
        client = get_client()
        rows_loaded = load_partition(client, "steamspy_silver", ds)
        return {"ds": ds, "rows_loaded": rows_loaded}

    load_ch = load_clickhouse()

    should_extract >> extract_task >> bronze >> silver >> load_ch


dag = steamspy()

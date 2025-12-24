"""SteamSpy silver-layer PySpark job."""

from spark.config.loader import JobConfig
from dags.src.tran.silver_transform import run_silver_job


def main(spark, config: JobConfig) -> None:
    run_silver_job(spark, config)

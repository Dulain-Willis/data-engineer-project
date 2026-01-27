import os

from dotenv import load_dotenv
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from src.tran.spark.spark_conf import apply_s3a_conf


def build_spark_session(app_name: str) -> SparkSession:
    load_dotenv()

    spark_master_url = os.getenv("SPARK_MASTER_URL")

    conf = SparkConf()
    apply_s3a_conf(conf)

    builder = SparkSession.builder.appName(app_name).config(conf=conf)
    if spark_master_url:
        builder = builder.master(spark_master_url)

    return builder.getOrCreate()

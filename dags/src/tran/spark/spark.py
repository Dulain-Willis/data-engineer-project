import os
import sys

# Add dags to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))

from dotenv import load_dotenv
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from src.tran.spark.spark_conf import apply_s3a_conf

load_dotenv()

spark_master_url = os.getenv("SPARK_MASTER_URL")

conf = SparkConf()
apply_s3a_conf(conf)

spark = (
    SparkSession.builder
    .appName("spark-session")
    .master(spark_master_url)
    .config(conf=conf)
    .getOrCreate()
)

print(f"spark version: {spark.version}")

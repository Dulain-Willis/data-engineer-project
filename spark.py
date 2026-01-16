import os

from dotenv import load_dotenv

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

load_dotenv()

s3_url: str = os.getenv('MINIO_URL')
s3_access_key: str = os.getenv('MINIO_ACCESS_KEY')
s3_secret_key: str = os.getenv('MINIO_SECRET_KEY')
spark_master_url: str = os.getenv('SPARK_MASTER_URL')

conf = SparkConf()

conf.set('spark.hadoop.fs.s3a.endpoint', s3_url)
conf.set('spark.hadoop.fs.s3a.access.key', s3_access_key)
conf.set('spark.hadoop.fs.s3a.secret.key', s3_secret_key)
conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
conf.set('spark.hadoop.fs.s3a.path.style.access', 'true')
conf.set('spark.jars', '<path-to-project>/jars/*')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262')

spark = (
    SparkSession.builder
    .appName('spark-session')
    .master(spark_master_url)
    .config(conf=conf)
    .getOrCreate()
)


print(f'spark version: {spark.version}')


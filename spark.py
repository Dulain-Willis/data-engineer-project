from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("sandbox")
    .getOrCreate()
)



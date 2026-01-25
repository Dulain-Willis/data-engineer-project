from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("appid", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("developer", StringType(), True),
    StructField("publisher", StringType(), True),
    StructField("score_rank", StringType(), True),
    StructField("owners", StringType(), True),
    StructField("average_forever", IntegerType(), True),
    StructField("average_2weeks", IntegerType(), True),
    StructField("median_forever", IntegerType(), True),
    StructField("median_2weeks", IntegerType(), True),
    StructField("ccu", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("initialprice", IntegerType(), True),
    StructField("discount", IntegerType(), True),
    StructField("tags", StringType(), True),
    StructField("languages", StringType(), True),
    StructField("genre", StringType(), True),
])


def main():
    spark = SparkSession.builder.appName("bronze-to-silver").getOrCreate()

    df = spark.read.schema(schema).json("s3a://steamspy-dev-raw/steamspy_json")

    df.write.mode("overwrite").parquet("s3a://steamspy-dev-silver/steamspy/")

    spark.stop()

if __name__ == "__main__":
    main()

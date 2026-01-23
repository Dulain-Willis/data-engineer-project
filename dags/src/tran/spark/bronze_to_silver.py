from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("appid", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("developer", StringType(), True),
    StructField("publisher", StringType(), True),
    StructField("score_rank", StringType(), True),  # Can be empty string
    StructField("owners", StringType(), True),      # Range like "0 - 20,000"
    StructField("average_forever", IntegerType(), True),
    StructField("average_2weeks", IntegerType(), True),
    StructField("median_forever", IntegerType(), True),
    StructField("median_2weeks", IntegerType(), True),
    StructField("ccu", IntegerType(), True),
    StructField("price", IntegerType(), True),      # Cents
    StructField("initialprice", IntegerType(), True),
    StructField("discount", IntegerType(), True),
    StructField("tags", StringType(), True),        # JSON string
    StructField("languages", StringType(), True),
    StructField("genre", StringType(), True),
])


def main():
    spark = SparkSession.builder.appName("bronze-to-silver").getOrCreate()

    # Read raw JSON from bronze with explicit schema (avoids memory-heavy inference)
    df = spark.read.schema(schema).json("s3a://steamspy-dev-raw/steamspy_json")

    # Transform: select/clean columns, cast types
    cleaned = df.select(
        df["appid"].cast("int"),
        df["name"],
        df["developer"],
        df["publisher"],
        df["owners"],
        df["price"].cast("int"),
    )

    # Write to silver as Parquet
    cleaned.write.mode("overwrite").parquet("s3a://steamspy-dev-silver/steamspy/")

    spark.stop()

if __name__ == "__main__":
    main()

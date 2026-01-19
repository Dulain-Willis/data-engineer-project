from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("bronze-to-silver").getOrCreate()

    # Read raw JSON from bronze
    df = spark.read.json("s3a://steamspy-dev-raw/steamspy_json")

    # Transform: select/clean columns, cast types
    cleaned = df.select(
        df["appid"].cast("int"),
        df["name"],
        df["developer"],
        df["publisher"],
        df["positive"].cast("int"),
        df["negative"].cast("int"),
        df["owners"],
        df["price"].cast("int"),
    )

    # Write to silver as Parquet
    cleaned.write.mode("overwrite").parquet("s3a://steamspy-dev-silver/steamspy/")

    spark.stop()

if __name__ == "__main__":
    main()

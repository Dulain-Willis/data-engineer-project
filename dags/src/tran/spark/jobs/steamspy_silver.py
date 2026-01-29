from src.tran.spark.utils.session import build_spark_session
from pyspark.sql.functions import from_json, explode, col, lit, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType
from pyspark.sql.window import Window

game_schema = StructType([
    StructField("appid", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("developer", StringType(), True),
    StructField("publisher", StringType(), True),
    StructField("score_rank", StringType(), True),
    StructField("positive", IntegerType(), True),
    StructField("negative", IntegerType(), True),
    StructField("userscore", IntegerType(), True),
    StructField("owners", StringType(), True),
    StructField("average_forever", IntegerType(), True),
    StructField("average_2weeks", IntegerType(), True),
    StructField("median_forever", IntegerType(), True),
    StructField("median_2weeks", IntegerType(), True),
    StructField("ccu", IntegerType(), True),
    StructField("price", StringType(), True),
    StructField("initialprice", StringType(), True),
    StructField("discount", StringType(), True),
])


def main():
    spark = build_spark_session("steamspy-silver")

    ds = spark.conf.get("spark.steamspy.ds")
    run_id = spark.conf.get("spark.steamspy.run_id")

    bronze_path = f"s3a://bronze/steamspy/normalized/dt={ds}/"
    df_bronze = spark.read.parquet(bronze_path)

    json_map = MapType(StringType(), StructType(game_schema))

    # Takes the payload column and makes the id a string and casts the value (json payload) to a struct from a string
    df_parsed = (
        df_bronze
        .withColumn(
            "struct_payload",
            from_json(col("payload"), json_map)
        )
    )

    # Takes what was a huge string (now struct) of JSON and makes each row its own JSON entry for an appid
    df_exploded = (
        df_parsed
        .select(
            explode(col("struct_payload")).alias("appid_key", "game"),
            col("ingestion_timestamp"),
            col("run_id"),
        )
    )

    # Goes to each row and unnests each JSON entry into actual columns
    df_flattened = (
        df_exploded
        .select(
            col("game.*"),
            col("run_id"),
            col("ingestion_timestamp"),
        )
    )

    window = Window.partitionBy("appid").orderBy(col("ingestion_timestamp").desc())
    df_deduped = df_flattened.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")

    df_final = df_deduped.withColumn("dt", lit(ds))

    output_path = f"s3a://silver/steamspy/dt={ds}/"
    df_final.write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()

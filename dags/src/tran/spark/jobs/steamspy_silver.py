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

    map_schema = MapType(StringType(), StructType(game_schema))

    df_parsed = df_bronze.withColumn(
        "games_map",
        from_json(col("payload"), map_schema)
    )

    df_exploded = df_parsed.select(
        explode(col("games_map")).alias("appid_key", "game"),
        col("ingestion_timestamp"),
        col("run_id"),
    )

    df_games = df_exploded.select(
        col("game.*"),
        col("run_id"),
        col("ingestion_timestamp"),
    )

    window = Window.partitionBy("appid").orderBy(col("ingestion_timestamp").desc())
    df_deduped = df_games.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")

    df_final = df_deduped.withColumn("dt", lit(ds))

    output_path = f"s3a://silver/steamspy/dt={ds}/"
    df_final.write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()

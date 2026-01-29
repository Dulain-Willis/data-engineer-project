from src.tran.spark.utils.session import build_spark_session
from pyspark.sql.functions import from_json, explode, col, get_json_object, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType

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
    spark = build_spark_session("steamspy-silver")
    
    run_id = spark.sparkContext.getConf().get("spark.steamspy.run_id")
    if not run_id:
        raise ValueError("spark.steamspy.run_id must be set in Spark config")
    
    bronze_path = f"s3a://bronze/bronze/steamspy/run_id={run_id}/"
    df_bronze = spark.read.parquet(bronze_path)
    
    df_parsed = df_bronze.withColumn(
        "games_map",
        from_json(col("payload"), MapType(StringType(), StringType()))
    )
    
    df_exploded = df_parsed.select(
        explode(col("games_map")).alias("appid_key", "game_json_string"),
        col("run_id"),
        col("ingestion_timestamp")
    )
    
    df_silver = df_exploded.withColumn(
        "game",
        from_json(col("game_json_string"), schema)
    ).select(
        col("game.*"),
        col("run_id"),
        col("ingestion_timestamp")
    )
    
    df_final = df_silver.select(
        "appid",
        "name",
        "developer",
        "publisher",
        "score_rank",
        "owners",
        "average_forever",
        "average_2weeks",
        "median_forever",
        "median_2weeks",
        "ccu",
        "price",
        "initialprice",
        "discount",
        "tags",
        "languages",
        "genre",
        "run_id",
        "ingestion_timestamp"
    )
    
    df_final.write.mode("overwrite").parquet("s3a://silver/steamspy/")
    
    spark.stop()

if __name__ == "__main__":
    main()

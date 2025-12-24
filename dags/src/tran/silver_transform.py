"""PySpark silver-layer transformation for SteamSpy payloads.

This job reads raw JSON from the bronze bucket, normalizes the structure,
performs lightweight cleaning, and writes partitioned Parquet suitable for
upstream dbt models.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark.config.loader import JobConfig


def _explode_games(raw_df: DataFrame) -> DataFrame:
    # SteamSpy returns a single JSON object keyed by appid, so the columns of the
    # ingested DataFrame correspond to each app entry. Convert the columns into
    # an array and explode to get one row per game.
    game_array = F.array(*[F.col(col_name) for col_name in raw_df.columns])
    return raw_df.select(F.explode(game_array).alias("game"))


def _select_columns(games_df: DataFrame) -> DataFrame:
    # Pick a stable set of attributes and standardize types.
    return games_df.select(
        F.col("game.appid").cast("int").alias("appid"),
        F.col("game.name").alias("name"),
        F.col("game.developer").alias("developer"),
        F.col("game.publisher").alias("publisher"),
        F.col("game.owners").alias("owners"),
        F.col("game.tags").alias("tags"),
        F.col("game.score_rank").cast("int").alias("score_rank"),
        F.col("game.positive").cast("int").alias("positive_reviews"),
        F.col("game.negative").cast("int").alias("negative_reviews"),
        F.col("game.average_forever").cast("int").alias("avg_mins_played_forever"),
        F.col("game.average_2weeks").cast("int").alias("avg_mins_played_recent"),
        F.current_timestamp().alias("ingested_at"),
    )


def _deduplicate(df: DataFrame) -> DataFrame:
    # Keep the most recent record per appid to protect downstream uniqueness.
    window_spec = Window.partitionBy("appid").orderBy(F.col("ingested_at").desc())
    return (
        df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )


def run_silver_job(spark: SparkSession, config: JobConfig) -> None:
    bucket_name = "steam"
    dataset_prefix = "steamspy"
    bronze_prefix = f"bronze/{dataset_prefix}"
    silver_prefix = f"silver/{dataset_prefix}"

    bronze_path = config.extras.get(
        "bronze_path", f"s3a://{bucket_name}/{bronze_prefix}/steamspy-payload.json"
    )
    silver_path = config.extras.get(
        "silver_path", f"s3a://{bucket_name}/{silver_prefix}/games"
    )

    raw_df = spark.read.option("multiline", "true").json(bronze_path)

    games_df = _explode_games(raw_df)
    selected_df = _select_columns(games_df)
    deduped_df = _deduplicate(selected_df)

    deduped_df.write.mode("overwrite").option("compression", "snappy").parquet(
        silver_path
    )


def main(spark: SparkSession, config: JobConfig) -> None:
    # Entrypoint for spark.jobs.run_job
    run_silver_job(spark, config)


if __name__ == "__main__":  # pragma: no cover - manual invocation helper
    session = SparkSession.builder.appName("steamspy-silver").getOrCreate()
    run_silver_job(session, JobConfig())
    session.stop()

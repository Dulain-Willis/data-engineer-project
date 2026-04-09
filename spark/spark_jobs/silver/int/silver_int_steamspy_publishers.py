"""Extracts individual publishers from silver_stg_games into a deduplicated dimension table.

Reads the publisher column from iceberg.steamspy.silver_stg_games, splits
multi-publisher strings using smart comma splitting (preserving business
suffixes like LLC, Inc., Ltd.), deduplicates, and assigns surrogate keys.

Writes to iceberg.steamspy.silver_int_publishers partitioned by dt.
"""
from pipelines.common.spark.session import build_spark_session
from pipelines.common.text import smart_split_comma
from pyspark.sql.functions import col, lit, explode, udf, trim, monotonically_increasing_id
from pyspark.sql.types import ArrayType, StringType


def main():
    spark = build_spark_session("steamspy-silver-int-publishers")

    ds = spark.conf.get("spark.steamspy.ds")

    df_stg = spark.table("iceberg.steamspy.silver_stg_games").filter(col("dt") == ds)

    split_udf = udf(smart_split_comma, ArrayType(StringType()))

    df_split = (
        df_stg
        .filter(col("publisher").isNotNull())
        .select(explode(split_udf(col("publisher"))).alias("publisher"))
    )

    df_cleaned = (
        df_split
        .withColumn("publisher", trim(col("publisher")))
        .filter(col("publisher") != "")
        .dropDuplicates(["publisher"])
    )

    df_final = (
        df_cleaned
        .withColumn("publisher_id", monotonically_increasing_id())
        .withColumn("dt", lit(ds))
        .select("publisher_id", "publisher", "dt")
    )

    table_name = "iceberg.steamspy.silver_int_publishers"

    if not spark.catalog.tableExists(table_name):
        print(f"Creating Iceberg table: {table_name}")
        df_final.writeTo(table_name).partitionedBy("dt").create()
    else:
        print(f"Overwriting Iceberg table: {table_name}")
        df_final.writeTo(table_name).overwritePartitions()

    count = spark.table(table_name).filter(col("dt") == ds).count()
    print(f"Iceberg table {table_name} contains {count} rows for dt={ds}")

    spark.stop()


if __name__ == "__main__":
    main()

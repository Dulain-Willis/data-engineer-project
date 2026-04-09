"""Extracts individual developers from silver_stg_games into a deduplicated dimension table.

Reads the developer column from iceberg.steamspy.silver_stg_games, splits
multi-developer strings using smart comma splitting (preserving business
suffixes like LLC, Inc., Ltd.), deduplicates, and assigns surrogate keys.

Writes to iceberg.steamspy.silver_int_developers partitioned by dt.
"""
from pipelines.common.spark.session import build_spark_session
from pipelines.common.text import smart_split_comma
from pyspark.sql.functions import col, lit, explode, udf, trim, monotonically_increasing_id
from pyspark.sql.types import ArrayType, StringType


def main():
    spark = build_spark_session("steamspy-silver-int-developers")

    ds = spark.conf.get("spark.steamspy.ds")

    df_stg = spark.table("iceberg.steamspy.silver_stg_games").filter(col("dt") == ds)

    split_udf = udf(smart_split_comma, ArrayType(StringType()))

    df_split = (
        df_stg
        .filter(col("developer").isNotNull())
        .select(explode(split_udf(col("developer"))).alias("developer"))
    )

    df_cleaned = (
        df_split
        .withColumn("developer", trim(col("developer")))
        .filter(col("developer") != "")
        .dropDuplicates(["developer"])
    )

    df_final = (
        df_cleaned
        .withColumn("developer_id", monotonically_increasing_id())
        .withColumn("dt", lit(ds))
        .select("developer_id", "developer", "dt")
    )

    table_name = "iceberg.steamspy.silver_int_developers"

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

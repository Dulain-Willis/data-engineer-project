from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pipelines.common.spark.session import build_spark_session


def create_extraction_metadata_table():
    """Create the extraction_batches metadata table if it doesn't exist."""
    spark = build_spark_session("init-extraction-metadata")

    schema = StructType([
        StructField("dt", StringType(), nullable=False),
        StructField("run_id", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),  # SUCCESS, FAILED, IN_PROGRESS
        StructField("committed_at", TimestampType(), nullable=False),
        StructField("pages_extracted", LongType(), nullable=True),
        StructField("extraction_duration_seconds", LongType(), nullable=True),
    ])

    empty_df = spark.createDataFrame([], schema)

    # Create Iceberg table partitioned by dt for efficient queries
    empty_df.writeTo("iceberg.steamspy.extraction_batches") \
        .using("iceberg") \
        .partitionedBy("dt") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("format-version", "2") \
        .createOrReplace()

    print("Created iceberg.steamspy.extraction_batches metadata table")
    spark.stop()


if __name__ == "__main__":
    create_extraction_metadata_table()

from pipelines.common.spark.session import build_spark_session
from pyspark.sql.functions import current_timestamp, to_date, lit, udf, input_file_name
from pyspark.sql.types import IntegerType
import re


def main():
    spark = build_spark_session("steamspy-bronze")

    ds = spark.conf.get("spark.steamspy.ds")
    run_id = spark.conf.get("spark.steamspy.run_id")

    landing_path = f"s3a://bronze/steamspy/raw/request=all/dt={ds}/run_id={run_id}/"

    # Read each full page of JSON from the API call as one huge string of text
    df = spark.read.text(landing_path, wholetext=True)

    # Rename the column with the JSON text from the default 'value' to 'payload'
    df = df.withColumnRenamed("value", "payload")

    # Treating the following as a raw string using r' meaning treat \'s literally find the literal text 'page=' and
    # then create a capture group () to mark what we want to extract. Match any digit \d specifically one or more +
    # and then the literal text .json with \.json to show it's a literal . not "any character"
    def extract_page(path):
        if not path:
            raise ValueError("File path is None or empty")
        match = re.search(r'page=(\d+)\.json', path)
        if not match:
            raise ValueError(f"Could not extract page number from: {path}")
        return int(match.group(1))

    extract_page_udf = udf(extract_page, IntegerType())

    df = (
        df.withColumn("source_file", input_file_name())
          .withColumn("page", extract_page_udf("source_file"))
          .withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("ingestion_date", to_date("ingestion_timestamp"))
          .withColumn("source", lit("steamspy"))
          .withColumn("run_id", lit(run_id))
          .withColumn("dt", lit(ds))
    )

    df_final = (
        df.select(
            "payload",
            "ingestion_timestamp",
            "ingestion_date",
            "source",
            "run_id",
            "dt",
            "page",
            "source_file"
        )
    )

    bronze_output_path = f"s3a://bronze/steamspy/normalized/dt={ds}/"
    df_final.write.mode("overwrite").parquet(bronze_output_path)

    spark.stop()


if __name__ == "__main__":
    main()

from src.tran.spark.utils.session import build_spark_session
from pyspark.sql.functions import current_timestamp, date, lit, col, udf, input_file_name
from pyspark.sql.types import StringType, IntegerType
import re

def main():
    spark = build_spark_session("steamspy-bronze")
    
    # run_id keeps each Airflow run's data in its own folder.
    run_id = spark.sparkContext.getConf().get("spark.steamspy.run_id")
    if not run_id:
        raise ValueError(
            "run_id is required. Pass spark.steamspy.run_id in Spark config."
        )
    
    landing_path = f"s3a://bronze/steamspy/raw/request=all/run_id={run_id}/"
    
    # There is a file for each page from the API call full of JSON. Create a
    # df that creates one column leaving the JSON as text for now
    df = spark.read.text(landing_path)
    
    # Rename 'value' to 'payload' for clarity
    df = df.withColumnRenamed("value", "payload")
    
    def extract_page(path):
        if path:
            match = re.search(r'page=(\d+)\.json', path)
            return int(match.group(1)) if match else None
        return None
    
    extract_page_udf = udf(extract_page, IntegerType())
    
    df = (
        df.withColumn("source_file", input_file_name())
          .withColumn("page", extract_page_udf(col("source_file")))
          .withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("ingestion_date", date("ingestion_timestamp"))
          .withColumn("source", lit("steamspy"))
          .withColumn("run_id", lit(run_id))
    )
    
    df_final = df.select(
        "payload",
        "ingestion_timestamp",
        "ingestion_date",
        "source",
        "run_id",
        "page",
        "source_file"
    )
    
    bronze_output_path = f"s3a://bronze/steamspy/normalized/run_id={run_id}/"
    df_final.write.mode("overwrite").parquet(bronze_output_path)
    
    spark.stop()

if __name__ == "__main__":
    main()

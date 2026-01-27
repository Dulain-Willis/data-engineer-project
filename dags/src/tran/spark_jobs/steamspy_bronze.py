from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date, lit, col, udf
from pyspark.sql.types import StringType, IntegerType
import re

def main():
    spark = SparkSession.builder.appName("steamspy-bronze").getOrCreate()
    
    # Get run_id from Spark config
    run_id = spark.sparkContext.getConf().get("spark.steamspy.run_id")
    if not run_id:
        raise ValueError("spark.steamspy.run_id must be set in Spark config")
    
    # Read landing JSON files as raw text (one row per file)
    landing_path = f"s3a://bronze/landing/steamspy/request=all/run_id={run_id}/"
    
    # Read as text files - this gives us a 'value' column with the file content
    df = spark.read.text(landing_path)
    
    # Rename 'value' to 'payload' for clarity
    df = df.withColumnRenamed("value", "payload")
    
    # Extract page number from path for traceability
    # Path format: landing/steamspy/request=all/run_id={run_id}/page=0000.json
    # Note: when reading with text(), we don't have path info, so we'll extract from input_file_name()
    from pyspark.sql.functions import input_file_name
    
    def extract_page(path):
        if path:
            match = re.search(r'page=(\d+)\.json', path)
            return int(match.group(1)) if match else None
        return None
    
    extract_page_udf = udf(extract_page, IntegerType())
    
    # Add metadata columns
    df = df.withColumn("source_file", input_file_name()) \
           .withColumn("page", extract_page_udf(col("source_file"))) \
           .withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("ingestion_date", date("ingestion_timestamp")) \
           .withColumn("source", lit("steamspy")) \
           .withColumn("run_id", lit(run_id))
    
    # Select final columns: payload + metadata
    df_final = df.select(
        "payload",
        "ingestion_timestamp",
        "ingestion_date",
        "source",
        "run_id",
        "page",
        "source_file"
    )
    
    # Write to bronze layer as Parquet
    bronze_output_path = f"s3a://bronze/bronze/steamspy/run_id={run_id}/"
    df_final.write.mode("overwrite").parquet(bronze_output_path)
    
    spark.stop()

if __name__ == "__main__":
    main()

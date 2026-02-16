from pyspark.sql import SparkSession
from pipelines.common.spark.session import build_spark_session
from extraction_metadata import register_extraction
import re


def backfill_metadata():
    """Scan S3 for existing raw data and register in metadata table."""
    spark = build_spark_session("backfill-extraction-metadata")

    # Base path for raw data
    raw_base = "s3a://bronze/steamspy/raw/request=all/"

    # Use Hadoop FileSystem API to list directories
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()

    # Get FileSystem instance
    uri = sc._jvm.java.net.URI(raw_base)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)

    # List dt partitions
    base_path = sc._jvm.org.apache.hadoop.fs.Path(raw_base)

    if not fs.exists(base_path):
        print(f"Base path does not exist: {raw_base}")
        spark.stop()
        return

    dt_paths = fs.listStatus(base_path)

    for dt_path in dt_paths:
        dt_name = dt_path.getPath().getName()

        # Extract dt value from partition (e.g., "dt=2026-02-15")
        dt_match = re.match(r'dt=(.+)', dt_name)
        if not dt_match:
            print(f"Skipping non-dt partition: {dt_name}")
            continue

        dt = dt_match.group(1)

        # List run_id partitions under this dt
        run_id_paths = fs.listStatus(dt_path.getPath())

        for run_id_path in run_id_paths:
            run_id_name = run_id_path.getPath().getName()

            # Extract run_id value from partition (e.g., "run_id=manual__2026-02-15T21:34:54+00:00")
            run_id_match = re.match(r'run_id=(.+)', run_id_name)
            if not run_id_match:
                print(f"Skipping non-run_id partition: {run_id_name}")
                continue

            run_id = run_id_match.group(1)

            # Count pages (JSON files starting with "page=")
            page_files = fs.listStatus(run_id_path.getPath())
            page_count = 0

            for page_file in page_files:
                file_name = page_file.getPath().getName()
                if file_name.startswith("page=") and file_name.endswith(".json"):
                    page_count += 1

            if page_count > 0:
                # Register as successful extraction
                print(f"Backfilling: dt={dt}, run_id={run_id}, pages={page_count}")
                register_extraction(
                    spark=spark,
                    dt=dt,
                    run_id=run_id,
                    status="SUCCESS",
                    pages_extracted=page_count
                )
            else:
                print(f"Skipping empty extraction: dt={dt}, run_id={run_id}")

    print("Backfill complete!")
    spark.stop()


if __name__ == "__main__":
    backfill_metadata()

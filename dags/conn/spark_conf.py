import os

# MinIO/S3A configuration for Spark
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def get_s3a_conf() -> dict:
    """Returns S3A config dict for SparkSubmitOperator."""
    return {
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": "true",
    }


def apply_s3a_conf(spark_conf):
    """Applies S3A config to a SparkConf object."""
    for key, value in get_s3a_conf().items():
        spark_conf.set(key, value)
    return spark_conf


def get_spark_resource_conf() -> dict:
    """Resource configuration for memory-constrained environments (5GB Docker pool).

    Total Spark footprint: ~2.8GB (leaves room for Airflow, MinIO, etc.)
    Memory breakdown: 1g + 384m (driver) + 1g + 384m (executor)
    """
    return {
        "spark.driver.memory": "1g",
        "spark.executor.memory": "1g",
        "spark.driver.memoryOverhead": "384m",
        "spark.executor.memoryOverhead": "384m",
        "spark.sql.shuffle.partitions": "8",  # Reduce from default 200
    }

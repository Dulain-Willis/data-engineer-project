import os

# MinIO/S3A configuration for Spark
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def get_s3a_conf() -> dict:
    # Returns S3A config dict for SparkSubmitOperator.
    return {
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": "true",
    }


def apply_s3a_conf(spark_conf):
    # Applies S3A config to a SparkConf object.
    for key, value in get_s3a_conf().items():
        spark_conf.set(key, value)
    return spark_conf


def get_spark_resource_conf() -> dict:
    # Resource configuration for memory-constrained environments (5GB Docker pool).
    # Total Spark footprint: ~2.8GB (leaves room for Airflow, MinIO, etc.)
    # Memory breakdown: 1g + 384m (driver) + 1g + 384m (executor)
    return {
        "spark.driver.memory": "1g",
        "spark.executor.memory": "1g",
        "spark.driver.memoryOverhead": "384m",
        "spark.executor.memoryOverhead": "384m",
        "spark.sql.shuffle.partitions": "8",  # Reduce from default 200
    }


def get_iceberg_catalog_conf() -> dict:
    # Returns Iceberg REST catalog configuration for Spark
    iceberg_rest_uri = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")

    return {
        # Catalog registration
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "rest",
        "spark.sql.catalog.iceberg.uri": iceberg_rest_uri,

        # Storage configuration - uses existing silver bucket with /iceberg/ prefix
        "spark.sql.catalog.iceberg.warehouse": "s3a://silver/iceberg/",

        # S3/MinIO integration
        "spark.sql.catalog.iceberg.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.iceberg.s3.endpoint": MINIO_ENDPOINT,
        "spark.sql.catalog.iceberg.s3.access-key-id": MINIO_ACCESS_KEY,
        "spark.sql.catalog.iceberg.s3.secret-access-key": MINIO_SECRET_KEY,
        "spark.sql.catalog.iceberg.s3.path-style-access": "true",

        # Performance tuning
        "spark.sql.catalog.iceberg.cache-enabled": "false",  # Disable for dev simplicity
    }

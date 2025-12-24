# SparkSession builder with MinIO/S3A defaults.
from __future__ import annotations

import os
from pyspark.sql import SparkSession

from spark.config.loader import JobConfig


def build_spark_session(app_name: str = "steam-pyspark", config: JobConfig | None = None) -> SparkSession:
    # Construct a SparkSession configured for MinIO access.
    #
    # Parameters
    # ----------
    # app_name: str
    #     Name to assign to the Spark application.
    # config: JobConfig | None
    #     Optional configuration object. If omitted, env defaults are used.

    cfg = config or JobConfig()

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", cfg.minio_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(cfg.minio_endpoint.startswith("https")),
        )
        .config("spark.hadoop.fs.s3a.access.key", cfg.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.minio_secret_key)
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.signing-algorithm", "S3SignerType")
    )

    # Allow callers to pass through extra Spark configs via environment if needed
    extra_spark_configs = {
        key.removeprefix("SPARK_CONFIG_").lower().replace("__", "."): value
        for key, value in os.environ.items()
        if key.startswith("SPARK_CONFIG_")
    }
    for conf_key, conf_value in extra_spark_configs.items():
        builder = builder.config(conf_key, conf_value)

    return builder.getOrCreate()

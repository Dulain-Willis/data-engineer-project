from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from typing import Optional
import logging

logger = logging.getLogger(__name__)
METADATA_TABLE = "iceberg.steamspy.extraction_batches"


def register_extraction(
    spark: SparkSession,
    dt: str,
    run_id: str,
    status: str,
    pages_extracted: Optional[int] = None,
    extraction_duration_seconds: Optional[int] = None
) -> None:
    """Register an extraction batch result in the metadata table."""
    metadata_row = {
        "dt": dt,
        "run_id": run_id,
        "status": status,
        "committed_at": current_timestamp(),
        "pages_extracted": pages_extracted,
        "extraction_duration_seconds": extraction_duration_seconds,
    }

    metadata_df = spark.createDataFrame([metadata_row])
    metadata_df.writeTo(METADATA_TABLE).using("iceberg").append()

    logger.info(f"Registered extraction: dt={dt}, run_id={run_id}, status={status}")


def get_latest_successful_extraction(spark: SparkSession, dt: str) -> Optional[str]:
    """Find the latest successful run_id for a given dt."""
    query = f"""
    SELECT run_id, committed_at
    FROM {METADATA_TABLE}
    WHERE dt = '{dt}' AND status = 'SUCCESS'
    ORDER BY committed_at DESC
    LIMIT 1
    """

    result = spark.sql(query).collect()

    if result:
        run_id = result[0]["run_id"]
        logger.info(f"Latest successful extraction for dt={dt}: run_id={run_id}")
        return run_id
    else:
        logger.warning(f"No successful extraction found for dt={dt}")
        return None


def get_raw_data_path(spark: SparkSession, dt: str, run_id: Optional[str] = None) -> str:
    """Construct raw data path, discovering latest run_id if not provided."""
    if run_id is None:
        run_id = get_latest_successful_extraction(spark, dt)
        if run_id is None:
            raise ValueError(f"No successful extraction found for dt={dt}. Run extraction first.")

    path = f"s3a://bronze/steamspy/raw/request=all/dt={dt}/run_id={run_id}/"
    logger.info(f"Raw data path for dt={dt}: {path}")
    return path

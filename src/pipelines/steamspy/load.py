from clickhouse_driver import Client
from pipelines.common.storage.minio_client import (
    minio_endpoint,
    minio_access_key,
    minio_secret_key,
)


def delete_partition(client: Client, table: str, dt: str) -> None:
    """Deletes a partition from the table for idempotent reload."""
    query = f"ALTER TABLE {table} DELETE WHERE dt = %(dt)s"
    client.execute(query, {"dt": dt})


def load_partition(client: Client, table: str, s3_path: str, dt: str) -> int:
    """
    Loads steamspy partition from MinIO parquet into ClickHouse.
    Returns the number of rows inserted.
    """
    delete_partition(client, table, dt)

    insert_query = f"""
    INSERT INTO {table}
    SELECT
        appid,
        name,
        developer,
        publisher,
        score_rank,
        positive,
        negative,
        userscore,
        owners,
        average_forever,
        average_2weeks,
        median_forever,
        median_2weeks,
        ccu,
        price,
        initialprice,
        discount,
        run_id,
        ingestion_timestamp,
        dt
    FROM s3(
        'http://{minio_endpoint}/{s3_path}/*.parquet',
        '{minio_access_key}',
        '{minio_secret_key}',
        'Parquet'
    )
    """
    client.execute(insert_query)

    count_result = client.execute(
        f"SELECT count(*) FROM {table} WHERE dt = %(dt)s",
        {"dt": dt}
    )
    return count_result[0][0] if count_result else 0

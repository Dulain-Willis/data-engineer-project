from io import BytesIO
from conn.minio_conn import minio_client

def upload_to_minio(
    bucket: str,
    object_name: str,
    raw_bytes: str,
    content_type: str,
):
    client = minio_client()

    stream = BytesIO(raw_bytes)

    client.put_object(
        bucket,
        object_name,
        stream,
        length=len(raw_bytes),
        content_type=content_type,
    )

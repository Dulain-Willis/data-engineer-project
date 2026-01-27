import requests
import time
from requests.exceptions import RequestException
from src.load.minio_loader import upload_to_minio

url = "https://steamspy.com/api.php"

request_type = "all"

def call_steamspy_api(bucket: str, run_id: str) -> int:
    pages_uploaded = 0
    page = 0

    while True:

        try:
            params = {
                "request": request_type,
                "page": page
            }
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            print(f"Successful Request for page {page}")
            data = response.json()

        except RequestException as e:
            print("Network error:", e)
            break

        if not data:
            print("This page is empty, ending process")
            break

        object_name = (
            f"steamspy/raw/request={request_type}/run_id={run_id}/page={page:04d}.json"
        )

        upload_to_minio(
            bucket=bucket,
            object_name=object_name,
            raw_bytes=response.content,
            content_type="application/json",
        )
        
        pages_uploaded += 1

        page += 1

        print("Sleeping for 60 seconds to respect rate limits")
        time.sleep(60)

    return pages_uploaded

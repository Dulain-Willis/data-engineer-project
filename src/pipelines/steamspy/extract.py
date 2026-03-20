import requests
import time
from requests.exceptions import RequestException, JSONDecodeError

from pipelines.common.storage.minio_loader import upload_to_minio

url = "https://steamspy.com/api.php"

request_type = "all"


def call_steamspy_api(bucket: str, ds: str, run_id: str) -> int:
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

            # Check for empty response (end of pagination)
            if not response.content:
                print(f"End of pagination reached at page {page}.")
                break

            data = response.json()

        except JSONDecodeError as e:
            print(f"Invalid JSON received on page {page}. Ending extraction.")
            break

        except RequestException as e:
            print(f"Network error on page {page}: {e}")
            break

        if not data:
            print("This page is empty, ending process")
            break

        object_name = (
            f"steamspy/raw/request={request_type}/dt={ds}/page={page:04d}.json"
        )

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                upload_to_minio(
                    bucket=bucket,
                    object_name=object_name,
                    raw_bytes=response.content,
                    content_type="application/json",
                )
                break
            except Exception as e:
                if attempt == max_retries:
                    raise
                wait = 2 ** attempt
                print(f"Upload failed for page {page} (attempt {attempt}/{max_retries}): {e}. Retrying in {wait}s.")
                time.sleep(wait)

        pages_uploaded += 1

        page += 1

        print("Sleeping for 60 seconds to respect rate limits")
        time.sleep(60)

    return pages_uploaded

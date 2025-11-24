import requests
import time
from requests.exceptions import RequestException

url = "https://steamspy.com/api.php"

params = {
    "request": "all",
    "page": 0
}

def get_steamspy_api():
    data_dict = {}

    while True:

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            print(f"Successful Request for page {params["page"]}")
            data = response.json()

        except RequestException as e:
            print("Network error:", e)
            break

        if not data:
            print("This page is empty, ending process")
            break

        data_dict.update(data)

        params["page"] += 1

        print("Sleeping for 60 seconds to respect rate limits")
        time.sleep(60)

    return data_dict

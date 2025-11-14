import requests
from requests.exceptions import RequestException

url = "https://steamspy.com/api.php"
params = {"request": "all"}

def get_steamspy_api():

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        print("Successful Request")
        return response.json()
    except RequestException as e:
        print("Network error:", e)
        return {}

get_steamspy_api()


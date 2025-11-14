import requests

url = "https://steamspy.com/api.php"

def get_steamspy_api:
   
    params = {"request": "all"}

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        print("Successful Request")
        return response.json()
    except: requests.exception.RequestException as e:
        print("Network error:", e)
        return {}


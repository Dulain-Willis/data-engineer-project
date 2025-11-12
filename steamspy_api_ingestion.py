import requests 

# 400 is bad 
# 200 is good

def get_games():
    url = 'https://steamspy.com/api.php?request=top100in2weeks'

    try:
        response = requests.get(url)
        if response.ok:
            game_details = response
            print("Successful Request")
            return game_details
        else:
            print("Error:", response.status_code)
    except requests.exceptions.RequestException as e:
        print("Network error:", e)

get_games()
        
        


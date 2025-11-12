import requests 
import psycopg
import json

def get_games():
    url = 'https://steamspy.com/api.php?request=top100in2weeks'

    try:
        response = requests.get(url)
        response.raise_for_status()

        print("Successful request")
        
        return response.json()

    except requests.exceptions.RequestException as e:
        print("Network error:", e)
        return None

def main(): 

    data = get_games()
    if not data:
        return

    #Reads .sql file with CREATE TABLE command for the steamspy api 
    with open("steamspy_schema.sql", "r") as s:
        create_table = s.read()

    dsn = "dbname=postgres user=postgres password=postgres host=localhost"

    #Connects to the steamspy database
    with psycopg.connect(dsn) as connector:

        #Opens a cursor to perform database operations
        with connector.cursor() as cursor:

            cursor.execute(create_table)

            insert_sql = """
                INSERT INTO steamspy (
                    appid,
                    payload
                )
                VALUES (
                    %s, 
                    %s
                )
                ON CONFLICT (appid) DO UPDATE
                SET payload = EXCLUDED.payload;
            """
            
            
            for key, value in data.items():
                appid = int(key)
                payload = json.dumps(value)

                cursor.execute(insert_sql, (appid, payload))

        connector.commit()
        
if __name__ == "__main__":
    main()

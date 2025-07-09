import requests
import time
import json
from snowflake_connection import get_snowflake_connection

def get_app_list():
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["applist"]["apps"]

def fetch_game_details(appid):
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get(str(appid), {}).get("success") and data[str(appid)]["data"].get("type") == "game":
            return data[str(appid)]["data"]
    except Exception as e:
        print(f"Error fetching appid {appid}: {e}")
    return None

def insert_game(conn, game):
    name = game.get("name", "")
    is_free = game.get("is_free", False)
    price = game.get("price_overview", {}).get("final", 0) / 100 if game.get("price_overview") else 0
    release_date = game.get("release_date", {}).get("date", "")
    developer = game.get("developers", [""])[0]
    publisher = game.get("publishers", [""])[0]
    genres = ", ".join([g.get("description", "") for g in game.get("genres", [])])
    categories = ", ".join([c.get("description", "") for c in game.get("categories", [])])
    platforms = json.dumps(game.get("platforms", {}))
    metacritic_score = game.get("metacritic", {}).get("score", None)
    recommendations = game.get("recommendations", {}).get("total", None)
    short_description = game.get("short_description", "")
    languages = game.get("supported_languages", "")

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_games (
            appid INT,
            name STRING,
            is_free BOOLEAN,
            price FLOAT,
            release_date STRING,
            developer STRING,
            publisher STRING,
            genres STRING,
            categories STRING,
            platforms STRING,
            metacritic_score INT,
            recommendations INT,
            short_description STRING,
            languages STRING
        )
    """)
    cursor.execute("""
        INSERT INTO raw_games (
            appid, name, is_free, price, release_date,
            developer, publisher, genres, categories,
            platforms, metacritic_score, recommendations,
            short_description, languages
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        game["steam_appid"], name, is_free, price, release_date,
        developer, publisher, genres, categories, platforms,
        metacritic_score, recommendations, short_description, languages
    ))
    cursor.close()

def main():
    conn = get_snowflake_connection()
    apps = get_app_list()

    for app in apps[:200]:  # adjust limit for testing/production
        appid = app["appid"]
        game = fetch_game_details(appid)
        if game:
            insert_game(conn, game)
            print(f"Inserted: {game['name']}")
        time.sleep(0.25)  # rate-limit friendly

    conn.close()

if __name__ == "__main__":
    main()

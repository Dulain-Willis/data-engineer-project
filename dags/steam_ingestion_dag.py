import requests
import time
import json
from datetime import datetime
from airflow.decorators import dag, task
from project_include.snowflake_connection import get_snowflake_connection

default_args = {
    "owner": "Dulain",
    "retries": 1,
}

@dag(
    dag_id="steam_ingestion_dag",
    start_date=datetime(2025, 7, 1),
    schedule=None,  # Set cron string here if you want to schedule later
    catchup=False,
    default_args=default_args,
    tags=["steam", "ingestion"],
)
def steam_ingestion():

    @task()
    def get_app_list():
        """Fetch the Steam app list and limit for dev"""
        url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()["applist"]["apps"][15000:20000]  # Limit for dev

    @task()
    def fetch_game_details(apps):
        """Fetch details for each Steam app in the list"""
        def fetch(appid):
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

        results = []
        for app in apps:
            details = fetch(app["appid"])
            if details:
                results.append(details)
                print(f"✔ Insertable: {details['name']}")
            time.sleep(1.5)  # Slow down to avoid rate limiting
        return results

    @task()
    def load_into_snowflake(games):
        """Upsert game data into Snowflake raw_games table"""
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_games (
                appid INT PRIMARY KEY,
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

        # Upsert each game
        merge_sql = """
            MERGE INTO raw_games AS target
            USING (SELECT ? AS appid, ? AS name, ? AS is_free, ? AS price, ? AS release_date,
                          ? AS developer, ? AS publisher, ? AS genres, ? AS categories,
                          ? AS platforms, ? AS metacritic_score, ? AS recommendations,
                          ? AS short_description, ? AS languages) AS source
            ON target.appid = source.appid
            WHEN MATCHED THEN UPDATE SET
                name = source.name,
                is_free = source.is_free,
                price = source.price,
                release_date = source.release_date,
                developer = source.developer,
                publisher = source.publisher,
                genres = source.genres,
                categories = source.categories,
                platforms = source.platforms,
                metacritic_score = source.metacritic_score,
                recommendations = source.recommendations,
                short_description = source.short_description,
                languages = source.languages
            WHEN NOT MATCHED THEN
                INSERT (appid, name, is_free, price, release_date,
                        developer, publisher, genres, categories,
                        platforms, metacritic_score, recommendations,
                        short_description, languages)
                VALUES (source.appid, source.name, source.is_free, source.price, source.release_date,
                        source.developer, source.publisher, source.genres, source.categories,
                        source.platforms, source.metacritic_score, source.recommendations,
                        source.short_description, source.languages)
        """

        for game in games:
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

            cursor.execute(merge_sql, (
                game["steam_appid"], name, is_free, price, release_date,
                developer, publisher, genres, categories, platforms,
                metacritic_score, recommendations, short_description, languages
            ))

        cursor.close()
        conn.close()

    # Task pipeline
    app_list = get_app_list()
    game_details = fetch_game_details(app_list)
    load_into_snowflake(game_details)

steam_ingestion()

import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables from .env file (if it exists)
load_dotenv()

def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER", "DA"),
        password=os.getenv("SNOWFLAKE_PASSWORD", "your_password_here"),
        account=os.getenv("SNOWFLAKE_ACCOUNT", "omc35850.us-east-1"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "STEAM_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "STEAM_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    )
    return conn

if __name__ == "__main__":
    conn = get_snowflake_connection()
    print("✅ Connected successfully!")
    conn.close()
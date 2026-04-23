from pathlib import Path
import os

import pandas as pd
import pymysql
from dotenv import load_dotenv

# Load the requested environment file from the project root.
BASE_DIR = Path(__file__).resolve().parent.parent
env_file_name = os.getenv("ENV_FILE", ".env.local")
env_path = BASE_DIR / env_file_name
load_dotenv(env_path, override=True)

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
USERNAME = os.getenv("USERNAME", "").strip()

# Use plain table names locally and username-prefixed tables on the shared class DB.
if MYSQL_HOST == "localhost" or not USERNAME:
    FLIGHTS_TABLE = "flights"
    WEATHER_TABLE = "weather_hourly"
    MERGED_TABLE = "merged_data"
else:
    FLIGHTS_TABLE = f"{USERNAME}_flights"
    WEATHER_TABLE = f"{USERNAME}_weather_hourly"
    MERGED_TABLE = f"{USERNAME}_merged_data"

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "cursorclass": pymysql.cursors.DictCursor,
    "connect_timeout": 10,
}


def get_connection():
    """Create and return a MySQL connection."""
    return pymysql.connect(**DB_CONFIG)


def load_csv_to_table(csv_path, table_name):
    """Load a CSV file into a target MySQL table."""
    df = pd.read_csv(csv_path)

    conn = get_connection()
    cur = conn.cursor()

    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)
    sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        values = [None if pd.isna(v) else v for v in row.tolist()]
        cur.execute(sql, values)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {csv_path} into {table_name}")


if __name__ == "__main__":
    load_csv_to_table("data/processed/cleaned_flights.csv", FLIGHTS_TABLE)
    load_csv_to_table("data/processed/cleaned_weather.csv", WEATHER_TABLE)
    load_csv_to_table("data/processed/merged_flights_weather.csv", MERGED_TABLE)
import os
import pandas as pd
import pymysql
from dotenv import load_dotenv
"""Load processed CSV outputs into MySQL.

This script reads environment-based database settings so the same code can load
into either the local development database or the class MySQL server.
"""

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "cursorclass": pymysql.cursors.DictCursor,
    "connect_timeout": 10,
}

"""Return a configured MySQL connection for the current environment."""
def get_connection():
    return pymysql.connect(**DB_CONFIG)

"""Load one processed CSV file into a MySQL table row by row."""
def load_csv_to_table(csv_path, table_name):
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
    load_csv_to_table("data/processed/cleaned_flights.csv", "flights")
    load_csv_to_table("data/processed/cleaned_weather.csv", "weather_hourly")
    load_csv_to_table("data/processed/merged_flights_weather.csv", "merged_data")
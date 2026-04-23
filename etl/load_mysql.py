from pathlib import Path
import os

import pandas as pd
import pymysql
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
env_file_name = os.getenv("ENV_FILE", ".env.local")
env_path = BASE_DIR / env_file_name
load_dotenv(env_path, override=True)

print(f"Using env file: {env_path}")
print(f"MYSQL_HOST={os.getenv('MYSQL_HOST')}")
print(f"MYSQL_USER={os.getenv('MYSQL_USER')}")
print(f"DB_NAME={os.getenv('DB_NAME')}")
print(f"USERNAME={os.getenv('USERNAME')}")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
USERNAME = os.getenv("USERNAME", "").strip()
TRUNCATE_FIRST = os.getenv("TRUNCATE_FIRST", "0") == "1"
BATCH_SIZE = 5000

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
    return pymysql.connect(**DB_CONFIG)


def ensure_tables_exist(cur):
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {FLIGHTS_TABLE} (
            flight_id INT AUTO_INCREMENT PRIMARY KEY,
            FL_DATE DATE,
            ORIGIN VARCHAR(5),
            DEST VARCHAR(5),
            CRS_DEP_TIME INT,
            DEP_DELAY FLOAT,
            CANCELLED TINYINT,
            CANCELLATION_CODE VARCHAR(5),
            sched_dep_hour_ts DATETIME,
            delayed_15plus TINYINT,
            season VARCHAR(20)
        )
        """
    )

    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {WEATHER_TABLE} (
            weather_id INT AUTO_INCREMENT PRIMARY KEY,
            airport VARCHAR(5),
            weather_hour DATETIME,
            precipitation FLOAT,
            visibility FLOAT,
            wind_speed FLOAT,
            wind_gust FLOAT,
            temperature FLOAT,
            humidity FLOAT,
            rain_flag TINYINT,
            snow_flag TINYINT,
            fog_flag TINYINT,
            thunder_flag TINYINT
        )
        """
    )

    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MERGED_TABLE} (
            row_id INT AUTO_INCREMENT PRIMARY KEY,
            FL_DATE DATE,
            ORIGIN VARCHAR(5),
            DEST VARCHAR(5),
            CRS_DEP_TIME INT,
            DEP_DELAY FLOAT,
            CANCELLED TINYINT,
            CANCELLATION_CODE VARCHAR(5),
            sched_dep_hour_ts DATETIME,
            delayed_15plus TINYINT,
            season VARCHAR(20),
            airport VARCHAR(5),
            weather_hour DATETIME,
            precipitation FLOAT,
            visibility FLOAT,
            wind_speed FLOAT,
            wind_gust FLOAT,
            temperature FLOAT,
            humidity FLOAT,
            rain_flag TINYINT,
            snow_flag TINYINT,
            fog_flag TINYINT,
            thunder_flag TINYINT,
            precip_flag TINYINT,
            low_visibility_flag TINYINT,
            high_wind_flag TINYINT
        )
        """
    )


def truncate_tables(cur):
    cur.execute(f"TRUNCATE TABLE {MERGED_TABLE}")
    cur.execute(f"TRUNCATE TABLE {WEATHER_TABLE}")
    cur.execute(f"TRUNCATE TABLE {FLIGHTS_TABLE}")


def load_csv_to_table(cur, conn, csv_path, table_name):
    df = pd.read_csv(csv_path)

    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)
    sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

    batch = []
    inserted = 0

    for _, row in df.iterrows():
        values = [None if pd.isna(v) else v for v in row.tolist()]
        batch.append(values)

        if len(batch) >= BATCH_SIZE:
            cur.executemany(sql, batch)
            conn.commit()
            inserted += len(batch)
            print(f"Inserted {inserted:,} rows into {table_name}")
            batch = []

    if batch:
        cur.executemany(sql, batch)
        conn.commit()
        inserted += len(batch)

    print(f"Loaded {inserted:,} rows into {table_name}")


def main():
    conn = get_connection()
    cur = conn.cursor()

    print(f"Using tables: {FLIGHTS_TABLE}, {WEATHER_TABLE}, {MERGED_TABLE}")

    ensure_tables_exist(cur)
    conn.commit()
    print("Verified tables exist.")

    if TRUNCATE_FIRST:
        truncate_tables(cur)
        conn.commit()
        print("Truncated existing table data.")

    load_csv_to_table(cur, conn, "data/processed/cleaned_flights.csv", FLIGHTS_TABLE)
    load_csv_to_table(cur, conn, "data/processed/cleaned_weather.csv", WEATHER_TABLE)
    load_csv_to_table(cur, conn, "data/processed/merged_flights_weather.csv", MERGED_TABLE)

    cur.close()
    conn.close()
    print("Remote MySQL load complete.")


if __name__ == "__main__":
    main()
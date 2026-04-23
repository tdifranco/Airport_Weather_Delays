from pathlib import Path
import os

import pymysql
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
env_file_name = os.getenv("ENV_FILE", ".env.school")
env_path = BASE_DIR / env_file_name
load_dotenv(env_path, override=True)

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "connect_timeout": 10,
}

USERNAME = os.getenv("USERNAME", "").strip()
if not USERNAME:
    raise ValueError("USERNAME is not set in env file.")

FLIGHTS_TABLE = f"{USERNAME}_flights"
WEATHER_TABLE = f"{USERNAME}_weather_hourly"
MERGED_TABLE = f"{USERNAME}_merged_data"

CREATE_FLIGHTS_SQL = f"""
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

CREATE_WEATHER_SQL = f"""
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

CREATE_MERGED_SQL = f"""
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

def main():
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()

    print(f"Connected to {os.getenv('DB_NAME')} on {os.getenv('MYSQL_HOST')}")
    print(f"Creating {FLIGHTS_TABLE}, {WEATHER_TABLE}, {MERGED_TABLE}")

    cur.execute(CREATE_FLIGHTS_SQL)
    cur.execute(CREATE_WEATHER_SQL)
    cur.execute(CREATE_MERGED_SQL)

    conn.commit()
    cur.close()
    conn.close()

    print("School tables created successfully.")

if __name__ == "__main__":
    main()
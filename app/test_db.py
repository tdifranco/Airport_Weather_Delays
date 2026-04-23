import os
from pathlib import Path

import pymysql
from dotenv import load_dotenv

# Project root = one level above the app folder
BASE_DIR = Path(__file__).resolve().parent.parent

# Default to local env, but allow override with ENV_FILE=.env.class
env_file_name = os.getenv("ENV_FILE", ".env.local")
env_path = BASE_DIR / env_file_name

# Load the requested env file and override any existing variables
load_dotenv(env_path, override=True)

print(f"Using env file: {env_path}")
print(f"MYSQL_HOST={os.getenv('MYSQL_HOST')}")
print(f"MYSQL_USER={os.getenv('MYSQL_USER')}")
print(f"DB_NAME={os.getenv('DB_NAME')}")

conn = pymysql.connect(
    host=os.getenv("MYSQL_HOST"),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database=os.getenv("DB_NAME"),
    connect_timeout=10,
)

print("Connected successfully")
conn.close()
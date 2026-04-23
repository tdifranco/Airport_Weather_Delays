import os
from pathlib import Path

import pymysql
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
env_file_name = os.getenv("ENV_FILE", ".env.local")
env_path = BASE_DIR / env_file_name
load_dotenv(env_path, override=True)


def get_connection():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("DB_NAME"),
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )
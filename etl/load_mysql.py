import pandas as pd
import pymysql

DB_CONFIG = {
    "host": "localhost",
    "user": "ia626app",
    "password": "ia626app123",
    "database": "flight_weather",
    "cursorclass": pymysql.cursors.DictCursor,
}

def get_connection():
    return pymysql.connect(**DB_CONFIG)


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
    load_csv_to_table("data/processed/merged_flights_weather.csv", "merged_data")
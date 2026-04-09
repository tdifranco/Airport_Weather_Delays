from flask import Flask, render_template, request
from db import get_connection

app = Flask(__name__)

@app.route("/")
def index():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT origin, AVG(dep_delay) AS avg_delay, AVG(cancelled) AS cancel_rate
        FROM merged_data
        GROUP BY origin
        ORDER BY avg_delay DESC
    """)
    airport_stats = cur.fetchall()

    cur.close()
    conn.close()
    return render_template("index.html", airport_stats=airport_stats)

@app.route("/airport")
def airport():
    airport_code = request.args.get("code", "ATL")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT origin, precipitation, visibility, wind_speed, dep_delay, cancelled
        FROM merged_data
        WHERE origin = %s
        LIMIT 200
    """, (airport_code,))
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return render_template("airport.html", airport_code=airport_code, rows=rows)

@app.route("/weather")
def weather():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            rain_flag,
            snow_flag,
            fog_flag,
            AVG(dep_delay) AS avg_delay,
            AVG(cancelled) AS cancel_rate
        FROM merged_data
        GROUP BY rain_flag, snow_flag, fog_flag
        ORDER BY avg_delay DESC
    """)
    weather_stats = cur.fetchall()

    cur.close()
    conn.close()
    return render_template("weather.html", weather_stats=weather_stats)

if __name__ == "__main__":
    app.run(debug=True)
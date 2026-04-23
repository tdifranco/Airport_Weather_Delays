from flask import Flask, render_template, request, redirect, url_for
"""Flask dashboard for exploring how weather relates to flight delays.

This module is the presentation/controller layer of the project.
It:
1. Reads filter inputs from the user interface.
2. Queries the MySQL merged_data table.
3. Aggregates the results into KPIs, tables, and chart-ready series.
4. Passes everything to Jinja templates for rendering.
"""

# Support both direct execution (python3 app/app.py)
# and package-style execution/imports (python3 -m app.app, pytest).
try:
    from .db import get_connection
except ImportError:
    from db import get_connection

app = Flask(__name__)
# Centralized map of supported weather filters. Each entry includes:
# - label: human-readable name for the UI
# - sql: SQL fragment used in WHERE clauses
WEATHER_FILTERS = {
    "all": {
        "label": "All Conditions",
        "sql": None,
    },
    "rain": {
        "label": "Rain",
        "sql": "COALESCE(rain_flag, 0) = 1",
    },
    "snow": {
        "label": "Snow",
        "sql": "COALESCE(snow_flag, 0) = 1",
    },
    "wind": {
        "label": "High Wind",
        "sql": "COALESCE(high_wind_flag, 0) = 1",
    },
    "fog": {
        "label": "Fog / Low Visibility",
        "sql": "(COALESCE(fog_flag, 0) = 1 OR COALESCE(low_visibility_flag, 0) = 1)",
    },
    "thunder": {
        "label": "Thunder",
        "sql": "COALESCE(thunder_flag, 0) = 1",
    },
    "clear": {
        "label": "Clear / Other",
        "sql": """
            COALESCE(rain_flag, 0) = 0
            AND COALESCE(snow_flag, 0) = 0
            AND COALESCE(high_wind_flag, 0) = 0
            AND COALESCE(fog_flag, 0) = 0
            AND COALESCE(thunder_flag, 0) = 0
            AND COALESCE(low_visibility_flag, 0) = 0
        """,
    },
}

"""Safely coerce chart/table values to floats.

    This helps when MySQL returns Decimal/None values and the data
    needs to be serialized into JSON-friendly chart arrays."""
def clean_num(value, default=0):
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default

"""Build a parameterized WHERE clause for airport + weather filters.

    Returns:
        tuple[str, list]: SQL WHERE fragment and parameter list.
"""
def build_where_clause(selected_airport="ALL", selected_weather="all"):
    clauses = []
    params = []

    if selected_airport and selected_airport != "ALL":
        clauses.append("ORIGIN = %s")
        params.append(selected_airport)

    weather_sql = WEATHER_FILTERS.get(selected_weather, WEATHER_FILTERS["all"])["sql"]
    if weather_sql:
        clauses.append(weather_sql)

    where_sql = ""
    if clauses:
        where_sql = " WHERE " + " AND ".join(f"({c})" for c in clauses)

    return where_sql, params

"""Build a WHERE clause using only the airport filter.

    Some dashboard sections compare weather conditions within a chosen airport,
    so they should not also apply the weather filter from the dropdown.
"""
def build_airport_only_clause(selected_airport="ALL"):
    clauses = []
    params = []

    if selected_airport and selected_airport != "ALL":
        clauses.append("ORIGIN = %s")
        params.append(selected_airport)

    where_sql = ""
    if clauses:
        where_sql = " WHERE " + " AND ".join(clauses)

    return where_sql, params

"""Return a sorted list of airport codes available in the merged dataset."""
def fetch_airports(cur):
    cur.execute("SELECT DISTINCT ORIGIN FROM merged_data ORDER BY ORIGIN")
    rows = cur.fetchall()
    return [row["ORIGIN"] for row in rows]


def fetch_kpis(cur, where_sql, params):
    """Return top-level KPI metrics for the current filtered view."""
    sql = f"""
        SELECT
            COUNT(*) AS total_flights,
            ROUND(AVG(DEP_DELAY), 1) AS avg_delay,
            ROUND(AVG(CANCELLED) * 100, 1) AS cancel_rate,
            ROUND(AVG(delayed_15plus) * 100, 1) AS delay15_rate
        FROM merged_data
        {where_sql}
    """
    sql = f"""
        SELECT
            COUNT(*) AS total_flights,
            ROUND(AVG(DEP_DELAY), 1) AS avg_delay,
            ROUND(AVG(CANCELLED) * 100, 1) AS cancel_rate,
            ROUND(AVG(delayed_15plus) * 100, 1) AS delay15_rate
        FROM merged_data
        {where_sql}
    """
    cur.execute(sql, params)
    return cur.fetchone()


def fetch_daily_trend(cur, where_sql, params):
    """Return daily trend metrics for average delay and cancellation rate.

    DATE(FL_DATE) is used consistently in both SELECT and GROUP BY to avoid
    ONLY_FULL_GROUP_BY issues in stricter MySQL configurations.
    """
    sql = f"""
        SELECT
            DATE(FL_DATE) AS day,
            ROUND(AVG(DEP_DELAY), 1) AS avg_delay,
            ROUND(AVG(CANCELLED) * 100, 1) AS cancel_rate
        FROM merged_data
        {where_sql}
        GROUP BY day
        ORDER BY day
    """
    sql = f"""
        SELECT
            DATE(FL_DATE) AS day,
            ROUND(AVG(DEP_DELAY), 1) AS avg_delay,
            ROUND(AVG(CANCELLED) * 100, 1) AS cancel_rate
        FROM merged_data
        {where_sql}
        GROUP BY day
        ORDER BY day
    """
    cur.execute(sql, params)
    return cur.fetchall()

def fetch_airport_comparison(cur, selected_weather="all"):
    where_sql, params = build_where_clause("ALL", selected_weather)
    sql = f"""
        SELECT
            ORIGIN,
            COUNT(*) AS flights,
            ROUND(AVG(DEP_DELAY), 1) AS avg_delay,
            ROUND(AVG(CANCELLED) * 100, 1) AS cancel_rate
        FROM merged_data
        {where_sql}
        GROUP BY ORIGIN
        ORDER BY avg_delay DESC
    """
    cur.execute(sql, params)
    return cur.fetchall()


def fetch_cause_breakdown(cur, selected_airport="ALL"):
    """Return likely delay/cancellation causes for the selected airport scope.

    This query uses UNION ALL to build a compact summary table across different
    weather condition flags without having to reshape the data in Python.
    """
    airport_where_sql, airport_params = build_airport_only_clause(selected_airport)

    sql = f"""
        SELECT * FROM (
            SELECT
                'Snow' AS cause,
                SUM(CASE WHEN COALESCE(snow_flag, 0) = 1 THEN 1 ELSE 0 END) AS affected_flights,
                ROUND(AVG(CASE WHEN COALESCE(snow_flag, 0) = 1 THEN DEP_DELAY END), 1) AS avg_delay,
                ROUND(AVG(CASE WHEN COALESCE(snow_flag, 0) = 1 THEN CANCELLED END) * 100, 1) AS cancel_rate
            FROM merged_data
            {airport_where_sql}

            UNION ALL

            SELECT
                'Rain' AS cause,
                SUM(CASE WHEN COALESCE(rain_flag, 0) = 1 THEN 1 ELSE 0 END) AS affected_flights,
                ROUND(AVG(CASE WHEN COALESCE(rain_flag, 0) = 1 THEN DEP_DELAY END), 1) AS avg_delay,
                ROUND(AVG(CASE WHEN COALESCE(rain_flag, 0) = 1 THEN CANCELLED END) * 100, 1) AS cancel_rate
            FROM merged_data
            {airport_where_sql}

            UNION ALL

            SELECT
                'High Wind' AS cause,
                SUM(CASE WHEN COALESCE(high_wind_flag, 0) = 1 THEN 1 ELSE 0 END) AS affected_flights,
                ROUND(AVG(CASE WHEN COALESCE(high_wind_flag, 0) = 1 THEN DEP_DELAY END), 1) AS avg_delay,
                ROUND(AVG(CASE WHEN COALESCE(high_wind_flag, 0) = 1 THEN CANCELLED END) * 100, 1) AS cancel_rate
            FROM merged_data
            {airport_where_sql}

            UNION ALL

            SELECT
                'Fog / Low Visibility' AS cause,
                SUM(
                    CASE
                        WHEN COALESCE(fog_flag, 0) = 1 OR COALESCE(low_visibility_flag, 0) = 1
                        THEN 1 ELSE 0
                    END
                ) AS affected_flights,
                ROUND(
                    AVG(
                        CASE
                            WHEN COALESCE(fog_flag, 0) = 1 OR COALESCE(low_visibility_flag, 0) = 1
                            THEN DEP_DELAY
                        END
                    ), 1
                ) AS avg_delay,
                ROUND(
                    AVG(
                        CASE
                            WHEN COALESCE(fog_flag, 0) = 1 OR COALESCE(low_visibility_flag, 0) = 1
                            THEN CANCELLED
                        END
                    ) * 100, 1
                ) AS cancel_rate
            FROM merged_data
            {airport_where_sql}

            UNION ALL

            SELECT
                'Thunder' AS cause,
                SUM(CASE WHEN COALESCE(thunder_flag, 0) = 1 THEN 1 ELSE 0 END) AS affected_flights,
                ROUND(AVG(CASE WHEN COALESCE(thunder_flag, 0) = 1 THEN DEP_DELAY END), 1) AS avg_delay,
                ROUND(AVG(CASE WHEN COALESCE(thunder_flag, 0) = 1 THEN CANCELLED END) * 100, 1) AS cancel_rate
            FROM merged_data
            {airport_where_sql}

            UNION ALL

            SELECT
                'Clear / Other' AS cause,
                SUM(
                    CASE
                        WHEN COALESCE(rain_flag, 0) = 0
                         AND COALESCE(snow_flag, 0) = 0
                         AND COALESCE(high_wind_flag, 0) = 0
                         AND COALESCE(fog_flag, 0) = 0
                         AND COALESCE(thunder_flag, 0) = 0
                         AND COALESCE(low_visibility_flag, 0) = 0
                        THEN 1 ELSE 0
                    END
                ) AS affected_flights,
                ROUND(
                    AVG(
                        CASE
                            WHEN COALESCE(rain_flag, 0) = 0
                             AND COALESCE(snow_flag, 0) = 0
                             AND COALESCE(high_wind_flag, 0) = 0
                             AND COALESCE(fog_flag, 0) = 0
                             AND COALESCE(thunder_flag, 0) = 0
                             AND COALESCE(low_visibility_flag, 0) = 0
                            THEN DEP_DELAY
                        END
                    ), 1
                ) AS avg_delay,
                ROUND(
                    AVG(
                        CASE
                            WHEN COALESCE(rain_flag, 0) = 0
                             AND COALESCE(snow_flag, 0) = 0
                             AND COALESCE(high_wind_flag, 0) = 0
                             AND COALESCE(fog_flag, 0) = 0
                             AND COALESCE(thunder_flag, 0) = 0
                             AND COALESCE(low_visibility_flag, 0) = 0
                            THEN CANCELLED
                        END
                    ) * 100, 1
                ) AS cancel_rate
            FROM merged_data
            {airport_where_sql}
        ) AS cause_summary
        WHERE affected_flights > 0
        ORDER BY avg_delay DESC
    """
    # The airport filter SQL block appears six times in the UNION query, so the
    # airport parameter list must also be repeated six times.
    params = airport_params * 6
    cur.execute(sql, params)
    return cur.fetchall()


def fetch_airport_weather_table(cur, selected_airport="ALL"):
    """Return a comparison table showing average delay by airport and weather type."""
    where_sql, params = build_airport_only_clause(selected_airport)
    sql = f"""
        SELECT
            ORIGIN,
            COUNT(*) AS flights,
            ROUND(AVG(DEP_DELAY), 1) AS avg_delay,
            ROUND(AVG(CANCELLED) * 100, 1) AS cancel_rate,
            ROUND(AVG(CASE WHEN COALESCE(rain_flag, 0) = 1 THEN DEP_DELAY END), 1) AS rain_delay,
            ROUND(AVG(CASE WHEN COALESCE(snow_flag, 0) = 1 THEN DEP_DELAY END), 1) AS snow_delay,
            ROUND(AVG(CASE WHEN COALESCE(high_wind_flag, 0) = 1 THEN DEP_DELAY END), 1) AS wind_delay,
            ROUND(
                AVG(
                    CASE
                        WHEN COALESCE(fog_flag, 0) = 1 OR COALESCE(low_visibility_flag, 0) = 1
                        THEN DEP_DELAY
                    END
                ), 1
            ) AS fog_delay
        FROM merged_data
        {where_sql}
        GROUP BY ORIGIN
        ORDER BY avg_delay DESC
    """
    cur.execute(sql, params)
    return cur.fetchall()


def to_chart_lists(rows, label_key, value_key):
    labels = [str(row[label_key]) for row in rows]
    values = [clean_num(row[value_key], 0) for row in rows]
    return labels, values


@app.route("/")
def dashboard():
    """Render the main dashboard page.

     The route:
        - reads query-string filters
        - queries MySQL for the filtered scope
        - prepares chart arrays
        - passes structured results into dashboard.html
    """
    selected_airport = request.args.get("airport", "ALL")
    selected_weather = request.args.get("weather", "all")
    if selected_weather not in WEATHER_FILTERS:
        selected_weather = "all"

    conn = get_connection()
    cur = conn.cursor()

    airports = fetch_airports(cur)

    filter_where_sql, filter_params = build_where_clause(selected_airport, selected_weather)
    airport_only_where_sql, airport_only_params = build_airport_only_clause(selected_airport)

    kpis = fetch_kpis(cur, filter_where_sql, filter_params)
    daily_trend = fetch_daily_trend(cur, filter_where_sql, filter_params)
    airport_comparison = fetch_airport_comparison(cur, selected_weather)
    cause_breakdown = fetch_cause_breakdown(cur, selected_airport)
    airport_weather_table = fetch_airport_weather_table(cur, selected_airport)

    cur.close()
    conn.close()

    trend_days, trend_avg_delay = to_chart_lists(daily_trend, "day", "avg_delay")
    _, trend_cancel_rate = to_chart_lists(daily_trend, "day", "cancel_rate")

    airport_labels, airport_avg_delay = to_chart_lists(airport_comparison, "ORIGIN", "avg_delay")
    _, airport_cancel_rate = to_chart_lists(airport_comparison, "ORIGIN", "cancel_rate")

    cause_labels, cause_avg_delay = to_chart_lists(cause_breakdown, "cause", "avg_delay")
    _, cause_cancel_rate = to_chart_lists(cause_breakdown, "cause", "cancel_rate")
    _, cause_flights = to_chart_lists(cause_breakdown, "cause", "affected_flights")

    top_causes = [
        row for row in cause_breakdown
        if row["cause"] != "Clear / Other"
        and clean_num(row["affected_flights"], 0) >= 25
        and row["avg_delay"] is not None
    ]
    top_causes = sorted(
        top_causes,
        key=lambda row: (clean_num(row["avg_delay"], 0), clean_num(row["cancel_rate"], 0)),
        reverse=True,
    )[:3]

    scope_label = "All 10 Airports" if selected_airport == "ALL" else selected_airport
    weather_label = WEATHER_FILTERS[selected_weather]["label"]

    return render_template(
        "dashboard.html",
        airports=airports,
        weather_filters=WEATHER_FILTERS,
        selected_airport=selected_airport,
        selected_weather=selected_weather,
        scope_label=scope_label,
        weather_label=weather_label,
        kpis=kpis,
        top_causes=top_causes,
        airport_weather_table=airport_weather_table,
        cause_breakdown=cause_breakdown,
        trend_days=trend_days,
        trend_avg_delay=trend_avg_delay,
        trend_cancel_rate=trend_cancel_rate,
        airport_labels=airport_labels,
        airport_avg_delay=airport_avg_delay,
        airport_cancel_rate=airport_cancel_rate,
        cause_labels=cause_labels,
        cause_avg_delay=cause_avg_delay,
        cause_cancel_rate=cause_cancel_rate,
        cause_flights=cause_flights,
    )


@app.route("/airport")
def airport_redirect():
    """Redirect legacy airport route usage into the main dashboard filter."""
    airport_code = request.args.get("code", "ALL")
    return redirect(url_for("dashboard", airport=airport_code))


@app.route("/weather")
def weather_redirect():
    """Redirect legacy weather route usage into the main dashboard filter."""
    weather_code = request.args.get("weather", "all")
    return redirect(url_for("dashboard", weather=weather_code))


if __name__ == "__main__":
    # Local development server only.
    app.run(debug=True)

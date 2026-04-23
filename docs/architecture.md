# Architecture

## High-Level Overview
This project is structured as an end-to-end analytics pipeline that moves from raw CSV data to a browser-based dashboard.

```text
Raw Flight CSV + Raw NOAA Weather CSVs
                ↓
             ETL Scripts
                ↓
    Cleaned / Merged Processed CSVs
                ↓
              MySQL
                ↓
         Flask + PyMySQL App
                ↓
     Jinja Templates + Chart.js UI
```

## Main Layers

### 1. Data Layer
The data layer consists of the raw flight and weather CSV files plus the processed CSV outputs created during ETL.

- `data/raw/` contains the original files.
- `data/processed/` contains cleaned and merged outputs.

### 2. ETL Layer
The ETL layer converts messy raw data into analysis-ready tables.

- `etl/clean_flights.py` standardizes flight dates and scheduled departure times.
- `etl/clean_weather.py` cleans NOAA weather files and aggregates them to one row per airport-hour.
- `etl/merge_data.py` joins flights to weather using origin airport and scheduled departure hour.
- `etl/load_mysql.py` loads processed results into MySQL.

### 3. Database Layer
MySQL stores the cleaned outputs so the Flask app can query structured data instead of raw CSVs.

Core tables:
- `flights`
- `weather_hourly`
- `merged_data`

### 4. Application Layer
The Flask application reads user-selected filters, runs SQL queries against `merged_data`, and prepares KPIs, charts, and tables.

- `app/db.py` handles environment-based database connection logic.
- `app/app.py` handles routes and query orchestration.

### 5. Presentation Layer
The Jinja templates and static assets render the final dashboard.

- `app/templates/base.html` contains the shared page shell.
- `app/templates/dashboard.html` contains the filter form, chart placeholders, and dashboard tables.
- `app/static/styles.css` controls the visual layout and styling.

## File Interaction Flow
1. Raw CSV files are placed into the data folder.
2. ETL scripts clean and reshape those files.
3. Processed CSVs are loaded into MySQL.
4. Flask queries MySQL based on user-selected filters.
5. Jinja templates render the returned results into HTML.
6. Chart.js visualizes the chart data in the browser.

## Design Rationale
The project is split into layers so that each responsibility stays isolated:
- ETL handles transformation.
- MySQL handles storage and querying.
- Flask handles backend logic.
- Templates and CSS handle presentation.

This modular structure makes the project easier to test, debug, explain, and extend.

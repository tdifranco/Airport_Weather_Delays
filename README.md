# Airport Weather Delays

## Overview
This project analyzes how weather conditions affect flight delays and cancellations at major U.S. airports. It combines flight performance data with hourly NOAA weather data, loads the results into MySQL, and presents the analysis in a Flask dashboard.

## Problem Statement
The main goal of this project is to identify which weather conditions are most associated with delays and cancellations and how those effects vary by airport.

## Tech Stack
- Python
- pandas
- NumPy
- MySQL
- PyMySQL
- Flask
- Jinja2
- Chart.js
- pytest

## Project Structure
- `data/raw/` – original flight and weather CSV files
- `data/processed/` – cleaned and merged data outputs
- `etl/clean_flights.py` – cleans and standardizes flight data
- `etl/clean_weather.py` – cleans hourly weather files and builds airport-hour weather features
- `etl/merge_data.py` – merges flights and weather
- `etl/load_mysql.py` – loads processed data into MySQL
- `app/app.py` – Flask routes and dashboard logic
- `app/db.py` – database connection logic
- `app/templates/` – Jinja HTML templates
- `app/static/styles.css` – dashboard styling
- `tests/test_app.py` – Flask route tests

## Data Sources
- Flight delay and cancellation dataset
- NOAA Local Climatological Data (LCD) for airport weather stations

## ETL Process
1. Clean the flight dataset and standardize departure timestamps.
2. Clean hourly weather files and convert observations into one row per airport-hour.
3. Merge weather with flights using origin airport and scheduled departure hour.
4. Load cleaned tables into MySQL for dashboard use.

## Database Tables
- `flights`
- `weather_hourly`
- `merged_data`

## Features
- Airport filter
- Weather-condition filter
- KPI cards for delay and cancellation metrics
- Daily trend charts
- Airport comparison charts
- Weather cause breakdowns

## Running the Project

### 1. Set environment variables
Create a `.env.local` file for local development and a `.env.class` file for the class database.

### 2. Run ETL
```bash
python3 etl/clean_flights.py
python3 etl/clean_weather.py
python3 etl/merge_data.py
ENV_FILE=.env.local python3 etl/load_mysql.py

### 3. Run App
python3 app/app.py

## Testing

This project uses Flask’s local test client and pytest.

Current tests check:
- dashboard route loads successfully
- airport filter route loads successfully
- weather filter route loads successfully

Run tests with:
```bash
python3 -m pytest
# Airport Weather Delays

An end-to-end analytics project that studies how weather conditions affect flight delays and cancellations at major U.S. airports.

This project combines flight performance data with hourly airport weather data, cleans and merges both datasets in Python, stores the results in MySQL, and serves an interactive dashboard through Flask.

---

## Project Overview

The goal of this project is to answer the question:

**Which weather conditions are most strongly associated with flight delays and cancellations, and how does that relationship vary by airport?**

To answer that, this project:

- cleans a flight delay and cancellation dataset
- cleans hourly airport weather datasets
- merges the two sources by airport and departure hour
- loads the results into MySQL
- displays the results in a Flask dashboard with filters, KPI cards, charts, and summary tables

The dashboard allows a user to:

- filter by airport
- filter by weather condition
- compare average delays across airports
- view cancellation trends
- identify likely weather-related delay drivers such as rain, snow, wind, fog, and low visibility

---

## Tech Stack

- Python
- pandas
- NumPy
- PyMySQL
- Flask
- Jinja2
- MySQL
- Chart.js
- pytest
- python-dotenv

---

## Project Structure

```text
Airport_Weather_Delays/
├── app/
│   ├── __init__.py
│   ├── app.py
│   ├── db.py
│   ├── db_connection_check.py
│   ├── templates/
│   │   ├── base.html
│   │   └── dashboard.html
│   └── static/
│       └── styles.css
├── data/
│   ├── raw/
│   └── processed/
├── docs/
│   ├── architecture.md
│   ├── setup.md
│   └── screenshots.md
├── etl/
│   ├── clean_flights.py
│   ├── clean_weather.py
│   ├── merge_data.py
│   ├── load_mysql.py
│   └── create_school_tables.py
├── tests/
│   └── test_app.py
├── .env.example
├── README.md
├── requirements.txt
└── pytest.ini
```

---

## Data Sources

This project uses two datasets:

### 1. Flight Data
The flight dataset includes fields such as:

- flight date
- origin airport
- destination airport
- scheduled departure time
- departure delay
- cancellation flag
- cancellation code

### 2. Hourly Airport Weather Data
The weather dataset includes fields such as:

- precipitation
- visibility
- wind speed
- wind gust
- temperature
- humidity
- weather condition indicators such as rain, snow, fog, and thunder

The two datasets are merged using:

- **origin airport**
- **scheduled departure hour**

---

## ETL Pipeline

The project is organized into a clear ETL workflow.

### `etl/clean_flights.py`
This script:

- reads the raw flight dataset
- standardizes date and time fields
- creates an hourly scheduled departure timestamp
- engineers analytical fields such as:
  - `delayed_15plus`
  - `season`

### `etl/clean_weather.py`
This script:

- reads hourly airport weather files
- standardizes weather columns
- creates one row per airport-hour
- builds weather condition flags such as:
  - rain
  - snow
  - fog
  - thunder
  - low visibility
  - high wind

### `etl/merge_data.py`
This script:

- merges the cleaned flight and weather datasets
- joins on origin airport and departure hour
- creates final analytical flags used in the dashboard

### `etl/load_mysql.py`
This script:

- loads processed CSV files into MySQL
- supports both local MySQL and school/remote MySQL using environment files
- uses plain table names locally
- uses username-prefixed tables on the shared school database

---

## How the Files Work Together

This project follows a layered architecture.

### Data Layer
- raw CSV files are stored in `data/raw/`
- cleaned and merged outputs are stored in `data/processed/`

### ETL Layer
- Python ETL scripts clean, transform, and merge the datasets
- processed outputs are prepared for database loading

### Database Layer
- MySQL stores the cleaned flight, weather, and merged tables
- the database connection is controlled through environment files

### Application Layer
- `app/app.py` handles Flask routes, filtering, and SQL query logic
- `app/db.py` handles the database connection
- Jinja templates render the dashboard HTML
- Chart.js renders the visualizations in the browser
- CSS controls the dashboard styling and layout

In short, the workflow is:

**raw data → cleaned data → merged data → MySQL → Flask dashboard**

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/tdifranco/Airport_Weather_Delays.git
cd Airport_Weather_Delays
```

### 2. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

## Environment Configuration

This project supports two database environments:

- local MySQL
- school / remote MySQL

Create environment files in the project root.

### `.env.local`

Use this file for local development.

```env
DB_NAME=flight_weather
MYSQL_USER=your_local_mysql_user
MYSQL_PASSWORD=your_local_mysql_password
MYSQL_HOST=localhost
USERNAME=your_username
```

### `.env.school`

Use this file for the school MySQL server.

```env
DB_NAME=ia626
MYSQL_USER=ia626
MYSQL_PASSWORD=your_school_mysql_password
MYSQL_HOST=mysql.clarksonmsda.org
USERNAME=your_username
```

### `.env.example`

You can keep this file in the repo as a template:

```env
DB_NAME=your_database_name
MYSQL_USER=your_mysql_username
MYSQL_PASSWORD=your_mysql_password
MYSQL_HOST=your_mysql_host
USERNAME=your_username
```

### Important

Do **not** commit real credentials to GitHub.

Your `.gitignore` should include:

```gitignore
.env
.env.local
.env.school
.venv/
__pycache__/
.DS_Store
```

---

## Raw Data Setup

Before running the ETL pipeline, place the raw data files into the correct folders under `data/raw/`.

Expected workflow:

- raw flight CSV file goes into `data/raw/`
- raw weather CSV files go into the correct weather folder under `data/raw/`

Then the ETL scripts will generate cleaned outputs in `data/processed/`.

---

## Running the ETL Pipeline

Run the ETL steps in this order:

```bash
python3 etl/clean_flights.py
python3 etl/clean_weather.py
python3 etl/merge_data.py
```

These scripts generate the processed CSV files needed for database loading.

---

## Running with Local MySQL
For Local MySQL it is stored in /usr/local/var/mysql and is too big of a file to commit

### 1. Load processed data into local MySQL

```bash
ENV_FILE=.env.local TRUNCATE_FIRST=1 python3 etl/load_mysql.py
```

### 2. Start the Flask app using local MySQL

```bash
ENV_FILE=.env.local python3 -m app.app
```

### 3. Open the dashboard

Go to:

```text
http://127.0.0.1:5000
```

---

## Running with School / Online MySQL

### 1. Load processed data into the school MySQL database

```bash
ENV_FILE=.env.school TRUNCATE_FIRST=1 python3 etl/load_mysql.py
```

### 2. Start the Flask app using the school MySQL database

```bash
ENV_FILE=.env.school python3 -m app.app
```

### 3. Open the dashboard

Go to:

```text
http://127.0.0.1:5000
```

---

## Testing

This project includes automated tests using Flask’s local test client and pytest.

Run tests with:

```bash
python3 -m pytest
```

Current tests verify:

- dashboard route loads successfully
- airport filter route loads successfully
- weather filter route loads successfully

---

## Dashboard Features

The dashboard includes:

- airport filter
- weather-condition filter
- KPI cards for:
  - total flights
  - average delay
  - cancellation rate
  - 15+ minute delay rate
- daily trend charts
- airport comparison charts
- likely delay-driver summary
- weather cause breakdown table
- airport-level weather delay summary table
## 1. Full Dashboard Overview

Shows the main dashboard with:
- filter controls
- KPI cards
- trend charts
- airport comparison charts

![Dashboard Overview](docs/HomeScreen.png)

---

## 2. Airport-Specific View

Example of the dashboard filtered to one airport.

![Dashboard Airport Filter](docs/AirportFilter.png)

---

## 3. Weather-Specific View

Example of the dashboard filtered to a weather condition.

![Dashboard Weather Filter](docs/WeatherFilter.png)

---

## 4. Airport + Weather Example

Example of a more specific filtered view.

![Dashboard Atlanta Rain](docs/ATLRain.png)

---

## 5. Cause Breakdown Table

Shows the weather-cause summary table comparing delay and cancellation rate.

![Dashboard Cause Breakdown](docs/CauseBreakdown.png)

---

## Troubleshooting

### MySQL connection issues
If the database connection fails:

- verify the correct `.env` file is being used
- verify the MySQL server is reachable
- confirm the database credentials are correct
- make sure the target tables exist before running the app

### School database table issues
If the app reports that `difrantc_merged_data` or other prefixed tables do not exist, run:

```bash
ENV_FILE=.env.school TRUNCATE_FIRST=1 python3 etl/load_mysql.py
```

### Duplicate rows
If you load data more than once without clearing tables first, duplicate rows may be inserted.

Use:

```bash
ENV_FILE=.env.local TRUNCATE_FIRST=1 python3 etl/load_mysql.py
```

or

```bash
ENV_FILE=.env.school TRUNCATE_FIRST=1 python3 etl/load_mysql.py
```

to reload cleanly.

---

## Known Limitations

- results depend on the date range included in the data
- broader seasonal conclusions require more months of flight and weather data
- online MySQL access may depend on network permissions or institution setup
- repeated loads can duplicate rows unless data is truncated first

---

## Future Improvements

Potential next steps include:

- adding more months of flight and weather data
- building predictive models for delays and cancellations
- deploying the dashboard online
- expanding automated testing for ETL and database logic
- adding more advanced dashboard filters and drill-down views

---

## Documentation

Additional documentation is available in the `docs/` folder:

- `docs/architecture.md`
- `docs/setup.md`
- `docs/screenshots.md`

---

## Author

**Thomas DiFranco**  
IA 626 Final Project
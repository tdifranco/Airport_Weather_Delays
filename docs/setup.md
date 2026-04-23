# Setup

## 1. Create a Virtual Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

## 2. Install Dependencies
```bash
pip install pandas numpy flask pymysql python-dotenv pytest
```

## 3. Configure Environment Files
Create these two files in the project root.

### `.env.local`
```env
DB_NAME=flight_weather
MYSQL_USER=ia626app
MYSQL_PASSWORD=ia626app123
MYSQL_HOST=localhost
USERNAME=difrantc
```

### `.env.class`
```env
DB_NAME=ia626
MYSQL_USER=ia626
MYSQL_PASSWORD=ia626clarkson
MYSQL_HOST=mysql.clarksonmsda.org
USERNAME=difrantc
```

## 4. Prepare Local MySQL
Make sure your local MySQL server is running and that the `flight_weather` database already exists.

## 5. Run ETL
```bash
python3 etl/clean_flights.py
python3 etl/clean_weather.py
python3 etl/merge_data.py
```

## 6. Load Into MySQL
### Local database
```bash
ENV_FILE=.env.local python3 etl/load_mysql.py
```

### Class database
```bash
ENV_FILE=.env.class python3 etl/load_mysql.py
```

## 7. Run the Flask App
### Local database
```bash
ENV_FILE=.env.local python3 -m app.app
```

### Class database
```bash
ENV_FILE=.env.class python3 -m app.app
```

The app runs locally at:
```text
http://127.0.0.1:5000
```

## 8. Run Tests
```bash
python3 -m pytest
```

## Troubleshooting
- If PyMySQL times out before authentication, the host is probably unreachable from your current network.
- If Flask cannot find templates, confirm `app/templates/` contains `base.html` and `dashboard.html`.
- If MySQL reports unknown columns, make sure your table schema matches the latest ETL output columns.

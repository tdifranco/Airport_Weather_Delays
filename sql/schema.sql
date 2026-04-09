CREATE DATABASE IF NOT EXISTS flight_weather;
USE flight_weather;

CREATE TABLE IF NOT EXISTS airports (
    airport_code VARCHAR(5) PRIMARY KEY,
    station_id VARCHAR(20) NOT NULL,
    airport_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS flights (
    flight_id INT AUTO_INCREMENT PRIMARY KEY,
    fl_date DATE,
    origin VARCHAR(5),
    dest VARCHAR(5),
    crs_dep_time INT,
    sched_dep_hour_ts DATETIME,
    dep_delay FLOAT,
    cancelled TINYINT,
    cancellation_code VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS weather_hourly (
    weather_id INT AUTO_INCREMENT PRIMARY KEY,
    airport_code VARCHAR(5),
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
);

CREATE TABLE IF NOT EXISTS merged_data (
    row_id INT AUTO_INCREMENT PRIMARY KEY,
    fl_date DATE,
    origin VARCHAR(5),
    dest VARCHAR(5),
    crs_dep_time INT,
    sched_dep_hour_ts DATETIME,
    dep_delay FLOAT,
    cancelled TINYINT,
    cancellation_code VARCHAR(5),
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
);
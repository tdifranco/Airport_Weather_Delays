DROP TABLE IF EXISTS difrantc_merged_data;
DROP TABLE IF EXISTS difrantc_weather_hourly;
DROP TABLE IF EXISTS difrantc_flights;

CREATE TABLE difrantc_flights (
    flight_id INT AUTO_INCREMENT PRIMARY KEY,
    FL_DATE DATE,
    ORIGIN VARCHAR(5),
    DEST VARCHAR(5),
    CRS_DEP_TIME INT,
    DEP_DELAY FLOAT,
    CANCELLED TINYINT,
    CANCELLATION_CODE VARCHAR(5),
    sched_dep_hour_ts DATETIME,
    delayed_15plus TINYINT,
    season VARCHAR(20)
);

CREATE TABLE difrantc_weather_hourly (
    weather_id INT AUTO_INCREMENT PRIMARY KEY,
    airport VARCHAR(5),
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

CREATE TABLE difrantc_merged_data (
    row_id INT AUTO_INCREMENT PRIMARY KEY,
    FL_DATE DATE,
    ORIGIN VARCHAR(5),
    DEST VARCHAR(5),
    CRS_DEP_TIME INT,
    DEP_DELAY FLOAT,
    CANCELLED TINYINT,
    CANCELLATION_CODE VARCHAR(5),
    sched_dep_hour_ts DATETIME,
    delayed_15plus TINYINT,
    season VARCHAR(20),
    airport VARCHAR(5),
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
    thunder_flag TINYINT,
    precip_flag TINYINT,
    low_visibility_flag TINYINT,
    high_wind_flag TINYINT
);
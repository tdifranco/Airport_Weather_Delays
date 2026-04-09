import pandas as pd
import numpy as np
import glob
import os

INPUT_FOLDER = "data/raw"
OUTPUT_FILE = "data/processed/cleaned_weather.csv"

AIRPORT_MAP = {
    "DFW": "USW00003927",
    "ATL": "USW00013874",
    "MCO": "USW00012815",
    "DEN": "USW00003017",
    "LAX": "USW00023174",
    "SEA": "USW00024233",
    "LAS": "USW00023169",
    "IAD": "USW00093738",
    "MDW": "USW00014819",
    "JFK": "USW00094789",
}
STATION_TO_AIRPORT = {v: k for k, v in AIRPORT_MAP.items()}


def clean_numeric(series):
    if series.dtype != "object":
        return pd.to_numeric(series, errors="coerce")

    s = series.astype(str).str.strip()
    s = s.replace({"": np.nan, "nan": np.nan, "NaN": np.nan, "M": np.nan, "T": "0.0"})
    s = s.str.extract(r"([-+]?\d*\.?\d+)")[0]
    return pd.to_numeric(s, errors="coerce")


def load_and_clean_weather_file(filepath):
    df = pd.read_csv(filepath, low_memory=False)

    needed_cols = [
        "STATION", "DATE", "NAME", "REPORT_TYPE",
        "HourlyPrecipitation", "HourlyPresentWeatherType", "HourlyVisibility",
        "HourlyWindSpeed", "HourlyWindGustSpeed", "HourlyDryBulbTemperature",
        "HourlyRelativeHumidity"
    ]
    existing_cols = [c for c in needed_cols if c in df.columns]
    df = df[existing_cols].copy()

    df = df[df["REPORT_TYPE"].isin(["FM-12", "FM-15", "FM-16"])].copy()
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df = df.dropna(subset=["DATE", "STATION"])
    df = df[(df["DATE"] >= "2025-01-01") & (df["DATE"] < "2025-02-01")].copy()

    numeric_cols = [
        "HourlyPrecipitation", "HourlyVisibility", "HourlyWindSpeed",
        "HourlyWindGustSpeed", "HourlyDryBulbTemperature", "HourlyRelativeHumidity"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = clean_numeric(df[col])

    df["HourlyPresentWeatherType"] = (
        df.get("HourlyPresentWeatherType", pd.Series(index=df.index, dtype="object"))
        .astype(str)
        .replace("nan", np.nan)
        .str.upper()
        .str.strip()
    )

    df["weather_hour"] = df["DATE"].dt.floor("h")
    df["airport"] = df["STATION"].map(STATION_TO_AIRPORT)
    df = df[df["airport"].notna()].copy()

    weather_text = df["HourlyPresentWeatherType"].fillna("")
    df["rain_flag"] = weather_text.str.contains("RA", regex=False).astype(int)
    df["snow_flag"] = weather_text.str.contains("SN", regex=False).astype(int)
    df["fog_flag"] = weather_text.str.contains("FG", regex=False).astype(int)
    df["thunder_flag"] = weather_text.str.contains("TS", regex=False).astype(int)

    weather_hourly = (
        df.groupby(["airport", "weather_hour"], as_index=False)
        .agg({
            "HourlyPrecipitation": "max",
            "HourlyVisibility": "mean",
            "HourlyWindSpeed": "mean",
            "HourlyWindGustSpeed": "max",
            "HourlyDryBulbTemperature": "mean",
            "HourlyRelativeHumidity": "mean",
            "rain_flag": "max",
            "snow_flag": "max",
            "fog_flag": "max",
            "thunder_flag": "max",
        })
    )

    weather_hourly = weather_hourly.rename(columns={
        "HourlyPrecipitation": "precipitation",
        "HourlyVisibility": "visibility",
        "HourlyWindSpeed": "wind_speed",
        "HourlyWindGustSpeed": "wind_gust",
        "HourlyDryBulbTemperature": "temperature",
        "HourlyRelativeHumidity": "humidity",
    })

    return weather_hourly


def main():
    weather_files = glob.glob(os.path.join(INPUT_FOLDER, "LCD_*.csv"))
    all_weather = []

    for file in weather_files:
        try:
            cleaned = load_and_clean_weather_file(file)
            all_weather.append(cleaned)
            print(f"Loaded {file}: {cleaned.shape}")
        except Exception as e:
            print(f"Error on {file}: {e}")

    weather = pd.concat(all_weather, ignore_index=True)
    weather = weather.drop_duplicates(subset=["airport", "weather_hour"]).copy()
    weather.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved cleaned weather to {OUTPUT_FILE}")
    print(weather.head())
    print(weather.shape)


if __name__ == "__main__":
    main()
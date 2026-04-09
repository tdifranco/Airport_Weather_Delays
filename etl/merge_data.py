import pandas as pd

FLIGHTS_FILE = "data/processed/cleaned_flights.csv"
WEATHER_FILE = "data/processed/cleaned_weather.csv"
OUTPUT_FILE = "data/processed/merged_flights_weather.csv"


def main():
    flights = pd.read_csv(FLIGHTS_FILE)
    weather = pd.read_csv(WEATHER_FILE)

    flights["sched_dep_hour_ts"] = pd.to_datetime(flights["sched_dep_hour_ts"], errors="coerce")
    weather["weather_hour"] = pd.to_datetime(weather["weather_hour"], errors="coerce")

    merged = flights.merge(
        weather,
        how="left",
        left_on=["ORIGIN", "sched_dep_hour_ts"],
        right_on=["airport", "weather_hour"],
    )

    if "precipitation" in merged.columns:
        merged["precip_flag"] = (merged["precipitation"].fillna(0) > 0).astype(int)
    if "visibility" in merged.columns:
        merged["low_visibility_flag"] = (merged["visibility"] < 3).fillna(False).astype(int)
    if "wind_speed" in merged.columns:
        merged["high_wind_flag"] = (merged["wind_speed"] >= 20).fillna(False).astype(int)

    merged.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved merged data to {OUTPUT_FILE}")
    print(merged.head())
    print(merged.shape)
    print("Missing weather rows:", merged["airport"].isna().sum() if "airport" in merged.columns else "N/A")


if __name__ == "__main__":
    main()
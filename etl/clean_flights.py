import pandas as pd
import numpy as np

"""Clean and standardize raw flight data for the dashboard pipeline.

This script:
1. Reads the raw flight CSV.
2. Filters the dataset to airports in scope.
3. Normalizes scheduled departure times into hourly timestamps.
4. Engineers analysis-friendly fields such as delayed_15plus.
5. Writes the cleaned result to data/processed/cleaned_flights.csv.
""" 
def parse_crs_dep_time(x):
    """Convert scheduled departure time from HHMM integer format into hour and minute."""
    if pd.isna(x):
        return np.nan, np.nan
    try:
        x = int(float(x))
        s = str(x).zfill(4)
        hh = int(s[:2])
        mm = int(s[2:])
        if hh > 23 or mm > 59:
            return np.nan, np.nan
        return hh, mm
    except Exception:
        return np.nan, np.nan


def main():
    df = pd.read_csv(INPUT_FILE, low_memory=False)

    df["FL_DATE"] = pd.to_datetime(df["FL_DATE"], errors="coerce")
    df = df[(df["FL_DATE"] >= "2025-01-01") & (df["FL_DATE"] < "2025-02-01")].copy()
    df = df[df["ORIGIN"].isin(TARGET_AIRPORTS)].copy()

    parsed = df["CRS_DEP_TIME"].apply(parse_crs_dep_time)
    df["sched_hour"] = parsed.apply(lambda t: t[0])
    df["sched_minute"] = parsed.apply(lambda t: t[1])

    df["scheduled_dep_datetime"] = (
        df["FL_DATE"]
        + pd.to_timedelta(df["sched_hour"].fillna(0), unit="h")
        + pd.to_timedelta(df["sched_minute"].fillna(0), unit="m")
    )
    df["sched_dep_hour_ts"] = df["scheduled_dep_datetime"].dt.floor("h")

    if "DEP_DELAY" in df.columns:
        df["DEP_DELAY"] = pd.to_numeric(df["DEP_DELAY"], errors="coerce")
        df["delayed_15plus"] = (df["DEP_DELAY"] >= 15).astype(int)

    if "CANCELLED" in df.columns:
        df["CANCELLED"] = pd.to_numeric(df["CANCELLED"], errors="coerce").fillna(0).astype(int)

    df["month"] = df["FL_DATE"].dt.month
    df["season"] = np.select(
        [
            df["month"].isin([12, 1, 2]),
            df["month"].isin([3, 4, 5]),
            df["month"].isin([6, 7, 8]),
            df["month"].isin([9, 10, 11]),
        ],
        ["Winter", "Spring", "Summer", "Fall"],
        default="Unknown",
    )

    keep_cols = [
        "FL_DATE", "ORIGIN", "DEST", "CRS_DEP_TIME", "DEP_DELAY", "CANCELLED",
        "CANCELLATION_CODE", "sched_dep_hour_ts", "delayed_15plus", "season"
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols].copy()

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved cleaned flights to {OUTPUT_FILE}")
    print(df.head())
    print(df.shape)


if __name__ == "__main__":
    main()
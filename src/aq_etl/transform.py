"""Transform: bronze JSON -> silver (clean hourly rows) -> gold (daily city metrics)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from aq_etl.aqi import categorise_series, pm25_sub_index

# Open-Meteo variable name -> warehouse column name (units in the name).
RENAME = {
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "humidity_pct",
    "precipitation": "precipitation_mm",
    "wind_speed_10m": "wind_speed_kmh",
    "pm2_5": "pm2_5",
    "pm10": "pm10",
    "nitrogen_dioxide": "no2",
    "ozone": "o3",
}
WEATHER_COLUMNS = ["temperature_c", "humidity_pct", "precipitation_mm", "wind_speed_kmh"]
AIR_COLUMNS = ["pm2_5", "pm10", "no2", "o3"]
MEASURE_COLUMNS = WEATHER_COLUMNS + AIR_COLUMNS

SILVER_COLUMNS = [
    "city", "ts_ist", "ts_utc", "date_ist", "hour",
    *MEASURE_COLUMNS,
    "aqi_pm25", "aqi_category",
]  # fmt: skip

GOLD_COLUMNS = [
    "city", "date",
    "avg_temp_c", "min_temp_c", "max_temp_c", "avg_humidity_pct",
    "total_precip_mm", "rainy_hours", "avg_wind_kmh",
    "avg_pm2_5", "max_pm2_5", "avg_pm10", "avg_no2", "avg_o3",
    "aqi_pm25", "aqi_category", "hours_observed",
]  # fmt: skip

RAIN_THRESHOLD_MM = 0.1  # an hour counts as "rainy" above this


def flatten_hourly(payloads: list[dict[str, Any]], city: str) -> pd.DataFrame:
    """Turn Open-Meteo payloads (parallel hourly arrays) into tidy one-row-per-hour data."""
    frames = [pd.DataFrame(p["hourly"]) for p in payloads if p.get("hourly", {}).get("time")]
    if not frames:
        return pd.DataFrame(columns=["city", "time"])
    df = pd.concat(frames, ignore_index=True).rename(columns=RENAME)
    df.insert(0, "city", city)
    for col in df.columns.drop(["city", "time"]):
        # API nulls arrive as None; coerce so every measure is float64 with NaN.
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    return df


def add_time_columns(df: pd.DataFrame, timezone: str = "Asia/Kolkata") -> pd.DataFrame:
    """Parse local timestamps and derive UTC time, local date and hour.

    We request data with `timezone=Asia/Kolkata`, so `time` is local wall-clock
    time without an offset. India has no daylight saving, so localising is
    unambiguous. Timestamps are stored naive (IST and UTC side by side) to keep
    DuckDB independent of the session time zone.
    """
    out = df.copy()
    local = pd.to_datetime(out.pop("time"), format="%Y-%m-%dT%H:%M")
    out["ts_ist"] = local
    out["ts_utc"] = local.dt.tz_localize(timezone).dt.tz_convert("UTC").dt.tz_localize(None)
    out["date_ist"] = local.dt.date
    out["hour"] = local.dt.hour.astype("int64")
    return out


def add_aqi_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add the PM2.5 sub-index and CPCB category (indicative, hourly)."""
    out = df.copy()
    out["aqi_pm25"] = out["pm2_5"].map(pm25_sub_index).astype("Int64")
    out["aqi_category"] = categorise_series(out["pm2_5"])
    return out


def build_silver(
    weather: pd.DataFrame, air: pd.DataFrame, timezone: str = "Asia/Kolkata"
) -> pd.DataFrame:
    """Join weather and air quality on (city, hour) and add derived columns.

    An outer join keeps hours that exist in only one source; the missing side
    becomes NaN and the data-quality null checks decide if that is acceptable.
    """
    if weather.empty and air.empty:
        return pd.DataFrame(columns=SILVER_COLUMNS)
    merged = weather.merge(air, on=["city", "time"], how="outer")
    for col in MEASURE_COLUMNS:  # a source may be missing entirely
        if col not in merged:
            merged[col] = float("nan")
    silver = add_aqi_columns(add_time_columns(merged, timezone))
    return silver[SILVER_COLUMNS].sort_values(["city", "ts_ist"]).reset_index(drop=True)


def build_gold(silver: pd.DataFrame) -> pd.DataFrame:
    """Aggregate silver hourly rows into one row per city per local date."""
    if silver.empty:
        return pd.DataFrame(columns=GOLD_COLUMNS)
    df = silver.assign(is_rainy=silver["precipitation_mm"] > RAIN_THRESHOLD_MM)
    gold = (
        df.groupby(["city", "date_ist"], as_index=False)
        .agg(
            avg_temp_c=("temperature_c", "mean"),
            min_temp_c=("temperature_c", "min"),
            max_temp_c=("temperature_c", "max"),
            avg_humidity_pct=("humidity_pct", "mean"),
            total_precip_mm=("precipitation_mm", "sum"),
            rainy_hours=("is_rainy", "sum"),
            avg_wind_kmh=("wind_speed_kmh", "mean"),
            avg_pm2_5=("pm2_5", "mean"),
            max_pm2_5=("pm2_5", "max"),
            avg_pm10=("pm10", "mean"),
            avg_no2=("no2", "mean"),
            avg_o3=("o3", "mean"),
            hours_observed=("pm2_5", "count"),
        )
        .rename(columns={"date_ist": "date"})
    )
    float_cols = gold.select_dtypes("float").columns
    gold[float_cols] = gold[float_cols].round(2)
    # Daily AQI uses the 24-hour mean, as CPCB defines it.
    gold["aqi_pm25"] = gold["avg_pm2_5"].map(pm25_sub_index).astype("Int64")
    gold["aqi_category"] = categorise_series(gold["avg_pm2_5"])
    gold["rainy_hours"] = gold["rainy_hours"].astype("int64")
    gold["hours_observed"] = gold["hours_observed"].astype("int64")
    return gold[GOLD_COLUMNS]

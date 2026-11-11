from datetime import date, timedelta

import pandas as pd
import pytest

from aq_etl.transform import SILVER_COLUMNS, build_gold, build_silver, flatten_hourly
from tests.conftest import load_fixture


@pytest.fixture
def silver() -> pd.DataFrame:
    weather = flatten_hourly([load_fixture("weather", "delhi")], "Delhi")
    air = flatten_hourly([load_fixture("air_quality", "delhi")], "Delhi")
    return build_silver(weather, air)


def test_flatten_produces_one_row_per_hour_with_renamed_float_columns():
    df = flatten_hourly([load_fixture("weather", "delhi")], "Delhi")
    assert len(df) == 48
    assert {"city", "time", "temperature_c", "humidity_pct"} <= set(df.columns)
    assert df["temperature_c"].dtype == "float64"
    assert (df["city"] == "Delhi").all()


def test_flatten_coerces_api_nulls_to_nan():
    payload = {"hourly": {"time": ["2026-11-01T00:00", "2026-11-01T01:00"], "pm2_5": [None, 12]}}
    df = flatten_hourly([payload], "Delhi")
    assert df["pm2_5"].isna().tolist() == [True, False]


def test_silver_has_expected_columns_and_unique_keys(silver):
    assert list(silver.columns) == SILVER_COLUMNS
    assert len(silver) == 48
    assert not silver.duplicated(["city", "ts_ist"]).any()


def test_timezone_handling_ist_is_utc_plus_530(silver):
    first = silver.iloc[0]
    assert first["ts_ist"] == pd.Timestamp("2026-11-01 00:00")
    assert first["ts_utc"] == pd.Timestamp("2026-10-31 18:30")
    assert (silver["ts_ist"] - silver["ts_utc"] == timedelta(hours=5, minutes=30)).all()
    assert first["date_ist"] == date(2026, 11, 1) and first["hour"] == 0


def test_outer_join_keeps_hours_missing_from_one_source():
    weather = pd.DataFrame(
        {"city": ["Pune"] * 2, "time": ["2026-11-01T00:00", "2026-11-01T01:00"],
         "temperature_c": [20.0, 21.0], "humidity_pct": [50.0, 55.0],
         "precipitation_mm": [0.0, 0.2], "wind_speed_kmh": [5.0, 6.0]}
    )  # fmt: skip
    air = pd.DataFrame(
        {"city": ["Pune"], "time": ["2026-11-01T00:00"],
         "pm2_5": [40.0], "pm10": [80.0], "no2": [20.0], "o3": [30.0]}
    )  # fmt: skip
    silver = build_silver(weather, air)
    assert len(silver) == 2
    assert pd.isna(silver.loc[1, "pm2_5"])
    assert silver.loc[0, "aqi_category"] == "Satisfactory"


def test_gold_aggregates_match_manual_calculation(silver):
    gold = build_gold(silver)
    assert len(gold) == 2  # 2 days for one city
    day1 = silver[silver["date_ist"] == date(2026, 11, 1)]
    row = gold[gold["date"] == date(2026, 11, 1)].iloc[0]
    assert row["avg_pm2_5"] == round(day1["pm2_5"].mean(), 2)
    assert row["max_temp_c"] == day1["temperature_c"].max()
    assert row["total_precip_mm"] == round(day1["precipitation_mm"].sum(), 2)
    assert row["hours_observed"] == 24


def test_gold_daily_category_uses_24h_mean():
    silver = pd.DataFrame(
        {"city": ["X"] * 2, "date_ist": [date(2026, 1, 1)] * 2,
         "temperature_c": [20.0, 22.0], "humidity_pct": [50.0, 60.0],
         "precipitation_mm": [0.0, 1.0], "wind_speed_kmh": [3.0, 5.0],
         "pm2_5": [20.0, 200.0], "pm10": [1.0, 1.0], "no2": [1.0, 1.0], "o3": [1.0, 1.0]}
    )  # fmt: skip
    row = build_gold(silver).iloc[0]
    assert row["avg_pm2_5"] == 110.0
    assert row["aqi_category"] == "Poor"  # from the mean, not the worst hour
    assert row["rainy_hours"] == 1

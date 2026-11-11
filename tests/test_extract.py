from datetime import date

import pytest

from aq_etl.config import City
from aq_etl.extract import (
    OpenMeteoClient,
    build_session,
    chunk_dates,
    extract_city,
    land_raw,
    read_bronze,
    split_by_day,
)
from tests.conftest import load_fixture


def test_chunk_dates_splits_inclusive_range():
    chunks = list(chunk_dates(date(2026, 1, 1), date(2026, 1, 10), chunk_days=4))
    assert chunks == [
        (date(2026, 1, 1), date(2026, 1, 4)),
        (date(2026, 1, 5), date(2026, 1, 8)),
        (date(2026, 1, 9), date(2026, 1, 10)),
    ]


def test_chunk_dates_single_day_and_invalid_range():
    assert list(chunk_dates(date(2026, 1, 1), date(2026, 1, 1), 31)) == [
        (date(2026, 1, 1), date(2026, 1, 1))
    ]
    with pytest.raises(ValueError):
        list(chunk_dates(date(2026, 1, 2), date(2026, 1, 1), 31))


def test_split_by_day_keeps_arrays_aligned():
    days = split_by_day(load_fixture("air_quality", "delhi"))
    assert sorted(days) == ["2026-11-01", "2026-11-02"]
    day = days["2026-11-02"]["hourly"]
    assert len(day["time"]) == len(day["pm2_5"]) == 24
    assert all(t.startswith("2026-11-02") for t in day["time"])


def test_land_and_read_bronze_roundtrip(tmp_path):
    city = City("New Delhi", 28.6, 77.2)
    paths = land_raw(tmp_path, "weather", city, load_fixture("weather", "delhi"))
    assert [p.name for p in paths] == ["2026-11-01.json", "2026-11-02.json"]
    assert paths[0].parent == tmp_path / "weather" / "new_delhi"

    payloads = read_bronze(tmp_path, "weather", city, date(2026, 11, 1), date(2026, 11, 3))
    assert len(payloads) == 2  # 3rd day missing -> skipped with a warning
    assert payloads[0]["city"] == "New Delhi" and "fetched_at" in payloads[0]


def test_extract_city_calls_both_sources_per_chunk(settings, fake_client):
    city = settings.cities[0]
    paths = extract_city(fake_client, settings, city, date(2026, 11, 1), date(2026, 11, 2))
    assert len(paths) == 4  # 2 sources x 2 days
    assert {c[0] for c in fake_client.calls} == {"weather", "air_quality"}


def test_weather_url_switches_to_archive_for_old_dates(settings):
    client = OpenMeteoClient(settings, session=build_session())
    today = date(2026, 11, 11)
    assert client.weather_url_for(date(2026, 10, 1), today) == settings.weather_archive_url
    assert client.weather_url_for(date(2026, 11, 10), today) == settings.weather_forecast_url


def test_session_retries_transient_errors():
    session = build_session(max_retries=3, backoff_factor=0.5)
    retry = session.get_adapter("https://example.com").max_retries
    assert retry.total == 3
    assert retry.backoff_factor == 0.5
    assert {429, 500, 503} <= set(retry.status_forcelist)


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class _RecordingSession:
    def __init__(self):
        self.requests = []

    def get(self, url, params, timeout):
        self.requests.append((url, params, timeout))
        return _FakeResponse({"hourly": {"time": []}})


def test_fetch_sends_expected_query(settings):
    session = _RecordingSession()
    client = OpenMeteoClient(settings, session=session)
    client.fetch("air_quality", settings.cities[0], date(2026, 11, 1), date(2026, 11, 2))
    url, params, timeout = session.requests[0]
    assert url == settings.air_quality_url
    assert params["hourly"] == "pm2_5,pm10,nitrogen_dioxide,ozone"
    assert params["timezone"] == "Asia/Kolkata"
    assert params["start_date"] == "2026-11-01" and params["end_date"] == "2026-11-02"
    assert timeout == settings.timeout_s
    with pytest.raises(ValueError):
        client.fetch("pollen", settings.cities[0], date(2026, 11, 1), date(2026, 11, 1))

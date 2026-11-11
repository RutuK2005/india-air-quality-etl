"""Extract: call the Open-Meteo APIs and land raw JSON in the bronze layer.

Bronze layout (one file per source / city / day):

    data/raw/<source>/<city>/<YYYY-MM-DD>.json

Re-fetching a day overwrites its file, so landing is idempotent too.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from aq_etl.config import City, Settings

log = logging.getLogger(__name__)

WEATHER = "weather"
AIR_QUALITY = "air_quality"
SOURCES = (WEATHER, AIR_QUALITY)


class Fetcher(Protocol):
    """Anything that can return an Open-Meteo style payload (real client or test fake)."""

    def fetch(self, source: str, city: City, start: date, end: date) -> dict[str, Any]: ...


def build_session(max_retries: int = 5, backoff_factor: float = 1.0) -> requests.Session:
    """Create a `requests.Session` that retries transient failures with exponential backoff.

    Retries happen on connection errors and on HTTP 429/5xx. With backoff_factor=1
    the waits are roughly 1s, 2s, 4s, 8s... and a `Retry-After` header is honoured.
    """
    retry = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": "india-air-quality-etl/0.1 (portfolio project)"})
    return session


def chunk_dates(start: date, end: date, chunk_days: int) -> Iterator[tuple[date, date]]:
    """Split an inclusive date range into consecutive chunks of at most `chunk_days` days."""
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


class OpenMeteoClient:
    """Thin HTTP client for the forecast, archive and air-quality endpoints."""

    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or build_session(settings.max_retries)
        self._last_request_at = 0.0

    def weather_url_for(self, end: date, today: date | None = None) -> str:
        """Pick the archive API for old dates and the forecast API for recent ones."""
        today = today or datetime.now(UTC).date()
        if end <= today - timedelta(days=self.settings.archive_lag_days):
            return self.settings.weather_archive_url
        return self.settings.weather_forecast_url

    def _throttle(self) -> None:
        """Sleep just long enough to keep `min_request_interval_s` between requests."""
        wait = self.settings.min_request_interval_s - (time.monotonic() - self._last_request_at)
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()

    def fetch(self, source: str, city: City, start: date, end: date) -> dict[str, Any]:
        """Fetch hourly data for one city and date range; raises on HTTP errors."""
        if source == WEATHER:
            url = self.weather_url_for(end)
            variables = self.settings.weather_variables
        elif source == AIR_QUALITY:
            url = self.settings.air_quality_url
            variables = self.settings.air_quality_variables
        else:
            raise ValueError(f"Unknown source: {source}")

        params = {
            "latitude": city.latitude,
            "longitude": city.longitude,
            "hourly": ",".join(variables),
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "timezone": self.settings.timezone,
        }
        self._throttle()
        log.debug("GET %s %s %s..%s", source, city.name, start, end)
        response = self.session.get(url, params=params, timeout=self.settings.timeout_s)
        response.raise_for_status()
        return response.json()


def split_by_day(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Split a multi-day payload into one payload per local date.

    Open-Meteo returns parallel arrays (`hourly.time`, `hourly.pm2_5`, ...). Each
    timestamp starts with its date ("2026-11-01T13:00"), so we group indexes by date.
    """
    hourly = payload.get("hourly", {})
    times: list[str] = hourly.get("time", [])
    indexes_by_day: dict[str, list[int]] = {}
    for i, ts in enumerate(times):
        indexes_by_day.setdefault(ts[:10], []).append(i)

    meta = {k: v for k, v in payload.items() if k != "hourly"}
    days = {}
    for day, idx in indexes_by_day.items():
        day_hourly = {key: [values[i] for i in idx] for key, values in hourly.items()}
        days[day] = {**meta, "hourly": day_hourly}
    return days


def bronze_path(raw_dir: Path, source: str, city: City, day: str) -> Path:
    """Path of the bronze file for one source/city/day."""
    return raw_dir / source / city.slug / f"{day}.json"


def land_raw(raw_dir: Path, source: str, city: City, payload: dict[str, Any]) -> list[Path]:
    """Write a payload to bronze, one JSON file per day. Returns the written paths."""
    fetched_at = datetime.now(UTC).isoformat(timespec="seconds")
    written = []
    for day, day_payload in split_by_day(payload).items():
        path = bronze_path(raw_dir, source, city, day)
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {"source": source, "city": city.name, "fetched_at": fetched_at, **day_payload}
        path.write_text(json.dumps(record), encoding="utf-8")
        written.append(path)
    return written


def extract_city(
    client: Fetcher, settings: Settings, city: City, start: date, end: date
) -> list[Path]:
    """Fetch both sources for a city over [start, end] in chunks and land them to bronze."""
    paths: list[Path] = []
    for source in SOURCES:
        for chunk_start, chunk_end in chunk_dates(start, end, settings.chunk_days):
            payload = client.fetch(source, city, chunk_start, chunk_end)
            paths.extend(land_raw(settings.raw_dir, source, city, payload))
    log.info("Extracted %s %s..%s (%d bronze files)", city.name, start, end, len(paths))
    return paths


def read_bronze(
    raw_dir: Path, source: str, city: City, start: date, end: date
) -> list[dict[str, Any]]:
    """Read the bronze files for one source/city across an inclusive date range."""
    payloads = []
    day = start
    while day <= end:
        path = bronze_path(raw_dir, source, city, day.isoformat())
        if path.exists():
            payloads.append(json.loads(path.read_text(encoding="utf-8")))
        else:
            log.warning("Missing bronze file %s", path)
        day += timedelta(days=1)
    return payloads

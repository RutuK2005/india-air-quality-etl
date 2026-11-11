"""Orchestration: extract -> transform -> data-quality gate -> load, with auditing."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from aq_etl import quality, warehouse
from aq_etl.config import City, Settings
from aq_etl.extract import AIR_QUALITY, WEATHER, Fetcher, OpenMeteoClient, extract_city, read_bronze
from aq_etl.transform import build_gold, build_silver, flatten_hourly

log = logging.getLogger(__name__)

# {city name: (first date, last date)} - each city can have its own range.
DateRanges = dict[str, tuple[date, date]]


@dataclass
class RunResult:
    """Summary of one pipeline run."""

    run_id: str
    status: str
    rows: dict[str, int] = field(default_factory=dict)
    ranges: DateRanges = field(default_factory=dict)
    error: str | None = None


def today_local(settings: Settings) -> date:
    """Today's date in the pipeline time zone (IST)."""
    return datetime.now(ZoneInfo(settings.timezone)).date()


def fixed_ranges(settings: Settings, start: date, end: date) -> DateRanges:
    """The same explicit date range for every configured city."""
    if end < start:
        raise ValueError(f"--end {end} is before --start {start}")
    return {city.name: (start, end) for city in settings.cities}


def plan_incremental(settings: Settings, watermarks: dict[str, date], today: date) -> DateRanges:
    """Work out what each city still needs, from the day after its watermark to yesterday.

    Yesterday is the last *complete* day. A city with no watermark starts
    `initial_lookback_days` back. Cities that are already up to date are skipped.
    """
    end = today - timedelta(days=1)
    plan: DateRanges = {}
    for city in settings.cities:
        last = watermarks.get(city.name)
        if last is None:
            start = end - timedelta(days=settings.initial_lookback_days - 1)
        else:
            start = last + timedelta(days=1)
        if start <= end:
            plan[city.name] = (start, end)
    return plan


def _transform(
    settings: Settings, cities: list[City], ranges: DateRanges
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Read bronze for the planned ranges and build silver and gold frames."""
    weather_frames, air_frames = [], []
    for city in cities:
        start, end = ranges[city.name]
        weather_payloads = read_bronze(settings.raw_dir, WEATHER, city, start, end)
        air_payloads = read_bronze(settings.raw_dir, AIR_QUALITY, city, start, end)
        weather_frames.append(flatten_hourly(weather_payloads, city.name))
        air_frames.append(flatten_hourly(air_payloads, city.name))
    weather = pd.concat(weather_frames, ignore_index=True)
    air = pd.concat(air_frames, ignore_index=True)
    silver = build_silver(weather, air, settings.timezone)
    return silver, build_gold(silver), len(weather) + len(air)


def run_pipeline(
    settings: Settings,
    ranges: DateRanges,
    mode: str,
    client: Fetcher | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
) -> RunResult:
    """Run the full ETL for the given per-city date ranges.

    Every run is written to `pipeline_runs`. If any step fails (HTTP error,
    data-quality failure, load error) the run is marked 'failed', nothing is
    loaded, watermarks stay where they were, and the exception is re-raised.
    """
    run_id = uuid.uuid4().hex[:12]
    result = RunResult(run_id=run_id, status="running", ranges=ranges)
    if not ranges:
        log.info("Nothing to do: all cities are up to date.")
        result.status = "skipped"
        return result

    own_connection = con is None
    con = con or warehouse.connect(settings.warehouse_path)
    overall_start = min(s for s, _ in ranges.values())
    overall_end = max(e for _, e in ranges.values())
    warehouse.start_run(con, run_id, mode, overall_start, overall_end)
    client = client or OpenMeteoClient(settings)
    cities = [c for c in settings.cities if c.name in ranges]

    try:
        # Extract (bronze)
        for city in cities:
            start, end = ranges[city.name]
            extract_city(client, settings, city, start, end)

        # Transform (silver + gold)
        silver, gold, bronze_rows = _transform(settings, cities, ranges)
        result.rows = {"bronze": bronze_rows, "silver": len(silver), "gold": len(gold)}

        # Data-quality gate: raises before anything touches the warehouse
        checks = quality.run_silver_checks(silver, settings.max_null_fraction)
        checks += quality.run_gold_checks(gold)
        quality.enforce(checks)

        # Load (idempotent upserts + watermarks in one transaction)
        loaded = {city: end for city, (_, end) in ranges.items()}
        warehouse.load_layers(con, silver, gold, run_id, loaded)
        result.status = "success"
    except Exception as exc:
        result.status = "failed"
        result.error = str(exc)
        log.error("Run %s failed: %s", run_id, exc)
        raise
    finally:
        warehouse.finish_run(con, run_id, result.status, result.rows, result.error)
        if own_connection:
            con.close()

    log.info("Run %s succeeded: %s", run_id, result.rows)
    return result

"""Load pipeline settings and the city list from a TOML file."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class City:
    """A location to collect data for."""

    name: str
    latitude: float
    longitude: float

    @property
    def slug(self) -> str:
        """Lower-case, filesystem-safe name used in bronze paths."""
        return self.name.lower().replace(" ", "_")


@dataclass(frozen=True)
class Settings:
    """All runtime settings. Defaults mirror `config.toml`."""

    cities: list[City]
    warehouse_path: Path = Path("data/warehouse.duckdb")
    raw_dir: Path = Path("data/raw")
    export_dir: Path = Path("data/exports")
    sql_dir: Path = Path("sql")
    timezone: str = "Asia/Kolkata"
    initial_lookback_days: int = 7
    chunk_days: int = 31
    min_request_interval_s: float = 0.3
    max_null_fraction: float = 0.1
    weather_forecast_url: str = "https://api.open-meteo.com/v1/forecast"
    weather_archive_url: str = "https://archive-api.open-meteo.com/v1/archive"
    air_quality_url: str = "https://air-quality-api.open-meteo.com/v1/air-quality"
    archive_lag_days: int = 6
    timeout_s: float = 30
    max_retries: int = 5
    weather_variables: list[str] = field(
        default_factory=lambda: [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
        ]
    )
    air_quality_variables: list[str] = field(
        default_factory=lambda: ["pm2_5", "pm10", "nitrogen_dioxide", "ozone"]
    )


def load_settings(path: str | Path = "config.toml") -> Settings:
    """Read a TOML config file into a `Settings` object.

    Unknown keys raise a TypeError, which catches typos in the config early.
    """
    with open(path, "rb") as fh:
        raw = tomllib.load(fh)

    cities = [City(**c) for c in raw.get("cities", [])]
    if not cities:
        raise ValueError(f"No [[cities]] defined in {path}")

    options: dict = {**raw.get("pipeline", {}), **raw.get("api", {})}
    for key in ("warehouse_path", "raw_dir", "export_dir", "sql_dir"):
        if key in options:
            options[key] = Path(options[key])
    return Settings(cities=cities, **options)

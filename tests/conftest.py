"""Shared fixtures. Tests never touch the network: FakeClient replays recorded JSON."""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from aq_etl.config import City, Settings
from aq_etl.extract import split_by_day

FIXTURES = Path(__file__).parent / "fixtures"
# The recorded fixtures cover these two days for Delhi and Mumbai.
FIXTURE_START = date(2026, 11, 1)
FIXTURE_END = date(2026, 11, 2)


class FakeClient:
    """Drop-in replacement for OpenMeteoClient that serves fixture files.

    `mutate` lets a test corrupt a payload (e.g. impossible humidity) to
    exercise the data-quality failure path.
    """

    def __init__(self, mutate: Callable[[str, dict[str, Any]], None] | None = None) -> None:
        self.mutate = mutate
        self.calls: list[tuple[str, str, date, date]] = []

    def fetch(self, source: str, city: City, start: date, end: date) -> dict[str, Any]:
        self.calls.append((source, city.name, start, end))
        payload = json.loads((FIXTURES / f"{source}_{city.slug}.json").read_text("utf-8"))
        days = split_by_day(payload)
        keep = [d for d in sorted(days) if start.isoformat() <= d <= end.isoformat()]
        hourly = {key: [] for key in payload["hourly"]}
        for day in keep:
            for key, values in days[day]["hourly"].items():
                hourly[key].extend(values)
        result = {**payload, "hourly": hourly}
        if self.mutate:
            result = copy.deepcopy(result)
            self.mutate(source, result)
        return result


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Settings pointing every path into a temporary directory."""
    return Settings(
        cities=[City("Delhi", 28.6139, 77.2090), City("Mumbai", 19.0760, 72.8777)],
        warehouse_path=tmp_path / "warehouse.duckdb",
        raw_dir=tmp_path / "raw",
        export_dir=tmp_path / "exports",
        sql_dir=Path(__file__).parents[1] / "sql",
        min_request_interval_s=0,
    )


@pytest.fixture
def fake_client() -> FakeClient:
    return FakeClient()


def load_fixture(source: str, city: str) -> dict[str, Any]:
    """Raw recorded payload for one source and city slug."""
    return json.loads((FIXTURES / f"{source}_{city}.json").read_text("utf-8"))

"""Data-quality gate: hand-written checks that run before anything is loaded.

Each check returns a `CheckResult`. `enforce` raises `DataQualityError` if any
check failed, which stops the pipeline before the warehouse is touched.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from aq_etl.transform import GOLD_COLUMNS, MEASURE_COLUMNS, SILVER_COLUMNS

# Plausible physical ranges (inclusive). Values outside these are data errors.
SILVER_RANGES: dict[str, tuple[float, float]] = {
    "temperature_c": (-30, 55),
    "humidity_pct": (0, 100),
    "precipitation_mm": (0, 500),
    "wind_speed_kmh": (0, 300),
    "pm2_5": (0, 2000),
    "pm10": (0, 3000),
    "no2": (0, 2000),
    "o3": (0, 2000),
    "hour": (0, 23),
}


@dataclass(frozen=True)
class CheckResult:
    """Outcome of one data-quality check."""

    name: str
    passed: bool
    details: str = ""


class DataQualityError(Exception):
    """Raised when one or more checks fail; carries the failed results."""

    def __init__(self, failures: list[CheckResult]) -> None:
        self.failures = failures
        lines = "; ".join(f"{f.name}: {f.details}" for f in failures)
        super().__init__(f"{len(failures)} data-quality check(s) failed: {lines}")


def check_not_empty(df: pd.DataFrame, table: str) -> CheckResult:
    """The frame must have at least one row."""
    return CheckResult(f"{table}.not_empty", len(df) > 0, f"{len(df)} rows")


def check_schema(df: pd.DataFrame, table: str, expected: list[str]) -> CheckResult:
    """All expected columns exist and measure columns are numeric."""
    missing = [c for c in expected if c not in df.columns]
    non_numeric = [
        c for c in MEASURE_COLUMNS if c in df.columns and not pd.api.types.is_numeric_dtype(df[c])
    ]
    problems = []
    if missing:
        problems.append(f"missing columns {missing}")
    if non_numeric:
        problems.append(f"non-numeric columns {non_numeric}")
    return CheckResult(f"{table}.schema", not problems, "; ".join(problems))


def check_not_null(df: pd.DataFrame, table: str, columns: list[str]) -> CheckResult:
    """Key columns may never be null."""
    counts = {c: int(df[c].isna().sum()) for c in columns if c in df}
    bad = {c: n for c, n in counts.items() if n}
    return CheckResult(f"{table}.keys_not_null", not bad, f"null counts {bad}" if bad else "")


def check_null_fraction(
    df: pd.DataFrame, table: str, columns: list[str], max_fraction: float
) -> CheckResult:
    """Measure columns may have some gaps, but not more than `max_fraction`."""
    if df.empty:
        return CheckResult(f"{table}.null_fraction", True)
    fractions = {c: float(df[c].isna().mean()) for c in columns if c in df}
    bad = {c: round(f, 3) for c, f in fractions.items() if f > max_fraction}
    details = f"over {max_fraction:.0%}: {bad}" if bad else ""
    return CheckResult(f"{table}.null_fraction", not bad, details)


def check_ranges(
    df: pd.DataFrame, table: str, ranges: dict[str, tuple[float, float]]
) -> CheckResult:
    """Non-null values must fall inside plausible ranges."""
    bad = {}
    for col, (low, high) in ranges.items():
        if col not in df:
            continue
        values = df[col].dropna()
        n_out = int(((values < low) | (values > high)).sum())
        if n_out:
            bad[col] = n_out
    return CheckResult(f"{table}.ranges", not bad, f"out-of-range counts {bad}" if bad else "")


def check_unique(df: pd.DataFrame, table: str, keys: list[str]) -> CheckResult:
    """The natural key must identify exactly one row."""
    dupes = int(df.duplicated(subset=keys).sum()) if not df.empty else 0
    return CheckResult(
        f"{table}.unique_{'_'.join(keys)}", dupes == 0, f"{dupes} duplicate rows" if dupes else ""
    )


def run_silver_checks(df: pd.DataFrame, max_null_fraction: float = 0.1) -> list[CheckResult]:
    """All checks for the silver (hourly) table."""
    return [
        check_not_empty(df, "silver"),
        check_schema(df, "silver", SILVER_COLUMNS),
        check_not_null(df, "silver", ["city", "ts_ist", "ts_utc", "date_ist"]),
        check_null_fraction(df, "silver", MEASURE_COLUMNS, max_null_fraction),
        check_ranges(df, "silver", SILVER_RANGES),
        check_unique(df, "silver", ["city", "ts_ist"]),
    ]


def run_gold_checks(df: pd.DataFrame) -> list[CheckResult]:
    """All checks for the gold (daily) table."""
    return [
        check_not_empty(df, "gold"),
        check_schema(df, "gold", GOLD_COLUMNS),
        check_not_null(df, "gold", ["city", "date"]),
        check_ranges(df, "gold", {"hours_observed": (0, 24), "avg_humidity_pct": (0, 100)}),
        check_unique(df, "gold", ["city", "date"]),
    ]


def enforce(results: list[CheckResult]) -> None:
    """Raise `DataQualityError` if any check failed."""
    failures = [r for r in results if not r.passed]
    if failures:
        raise DataQualityError(failures)

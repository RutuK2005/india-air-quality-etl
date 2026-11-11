"""Indian National AQI (CPCB) helpers for PM2.5.

CPCB breakpoints for PM2.5 (24-hour average, ug/m3):

    Category       PM2.5       AQI sub-index
    Good           0-30        0-50
    Satisfactory   31-60       51-100
    Moderate       61-90       101-200
    Poor           91-120      201-300
    Very Poor      121-250     301-400
    Severe         250+        401-500

CPCB defines AQI on a 24-hour average, so the daily (gold) category is the
"official-style" one; the hourly category in silver is only indicative.
"""

from __future__ import annotations

import math

import pandas as pd

# (upper concentration, upper AQI, category). Lower bounds are the previous row's
# upper bounds, which makes the scale continuous for non-integer concentrations.
# The Severe upper bound of 380 follows common CPCB calculator implementations.
PM25_BREAKPOINTS: list[tuple[float, float, str]] = [
    (30, 50, "Good"),
    (60, 100, "Satisfactory"),
    (90, 200, "Moderate"),
    (120, 300, "Poor"),
    (250, 400, "Very Poor"),
    (380, 500, "Severe"),
]
CATEGORIES = [category for _, _, category in PM25_BREAKPOINTS]


def pm25_category(pm25: float | None) -> str | None:
    """Return the CPCB category for one PM2.5 value (None for missing/negative)."""
    if pm25 is None or math.isnan(pm25) or pm25 < 0:
        return None
    for upper, _, category in PM25_BREAKPOINTS:
        if pm25 <= upper:
            return category
    return "Severe"


def pm25_sub_index(pm25: float | None) -> int | None:
    """Linear interpolation of the PM2.5 sub-index, capped at 500."""
    if pm25 is None or math.isnan(pm25) or pm25 < 0:
        return None
    low_c, low_i = 0.0, 0.0
    for high_c, high_i, _ in PM25_BREAKPOINTS:
        if pm25 <= high_c:
            return round(low_i + (pm25 - low_c) * (high_i - low_i) / (high_c - low_c))
        low_c, low_i = high_c, high_i
    return 500


def categorise_series(pm25: pd.Series) -> pd.Series:
    """Vectorised `pm25_category` for a pandas Series."""
    bins = [0, *[upper for upper, _, _ in PM25_BREAKPOINTS[:-1]], math.inf]
    return pd.cut(pm25, bins=bins, labels=CATEGORIES, include_lowest=True).astype("string")

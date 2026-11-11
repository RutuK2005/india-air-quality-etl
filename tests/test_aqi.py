import math

import pandas as pd
import pytest

from aq_etl.aqi import categorise_series, pm25_category, pm25_sub_index


@pytest.mark.parametrize(
    ("pm25", "expected"),
    [
        (0, "Good"),
        (30, "Good"),
        (30.1, "Satisfactory"),
        (60, "Satisfactory"),
        (75, "Moderate"),
        (90, "Moderate"),
        (91, "Poor"),
        (120, "Poor"),
        (121, "Very Poor"),
        (250, "Very Poor"),
        (251, "Severe"),
        (900, "Severe"),
    ],
)
def test_pm25_category_breakpoints(pm25, expected):
    assert pm25_category(pm25) == expected


@pytest.mark.parametrize("bad", [None, math.nan, -1])
def test_pm25_category_missing_or_invalid(bad):
    assert pm25_category(bad) is None
    assert pm25_sub_index(bad) is None


@pytest.mark.parametrize(
    ("pm25", "expected"),
    [
        (0, 0),
        (15, 25),
        (30, 50),
        (45, 75),
        (60, 100),
        (75, 150),
        (90, 200),
        (250, 400),
        (1000, 500),
    ],
)
def test_pm25_sub_index_interpolates_between_breakpoints(pm25, expected):
    assert pm25_sub_index(pm25) == expected


def test_vectorised_categories_match_scalar_function():
    values = pd.Series([0, 29.9, 30, 45, 61, 95, 200, 300, None])
    vectorised = categorise_series(values).tolist()
    scalar = [pm25_category(v) for v in values]
    assert [None if pd.isna(v) else v for v in vectorised] == scalar

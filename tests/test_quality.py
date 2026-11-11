import numpy as np
import pandas as pd
import pytest

from aq_etl.quality import (
    DataQualityError,
    enforce,
    run_gold_checks,
    run_silver_checks,
)
from aq_etl.transform import build_gold, build_silver, flatten_hourly
from tests.conftest import load_fixture


@pytest.fixture
def silver() -> pd.DataFrame:
    weather = flatten_hourly([load_fixture("weather", "mumbai")], "Mumbai")
    air = flatten_hourly([load_fixture("air_quality", "mumbai")], "Mumbai")
    return build_silver(weather, air)


def failed(results):
    return {r.name for r in results if not r.passed}


def test_clean_data_passes_all_checks(silver):
    assert failed(run_silver_checks(silver)) == set()
    assert failed(run_gold_checks(build_gold(silver))) == set()


def test_out_of_range_humidity_fails(silver):
    silver.loc[3, "humidity_pct"] = 140
    assert failed(run_silver_checks(silver)) == {"silver.ranges"}


def test_duplicate_city_hour_fails(silver):
    duplicated = pd.concat([silver, silver.head(2)], ignore_index=True)
    assert "silver.unique_city_ts_ist" in failed(run_silver_checks(duplicated))


def test_null_key_fails(silver):
    silver.loc[0, "city"] = None
    assert "silver.keys_not_null" in failed(run_silver_checks(silver))


def test_too_many_nulls_in_a_measure_fails(silver):
    silver.loc[: len(silver) // 2, "pm2_5"] = np.nan
    assert "silver.null_fraction" in failed(run_silver_checks(silver, max_null_fraction=0.1))


def test_missing_column_and_empty_frame_fail(silver):
    assert "silver.schema" in failed(run_silver_checks(silver.drop(columns=["o3"])))
    assert "silver.not_empty" in failed(run_silver_checks(silver.iloc[0:0]))


def test_enforce_raises_with_all_failures(silver):
    silver.loc[0, "humidity_pct"] = -5
    silver.loc[1, "city"] = None
    with pytest.raises(DataQualityError) as exc_info:
        enforce(run_silver_checks(silver))
    names = {f.name for f in exc_info.value.failures}
    assert names == {"silver.ranges", "silver.keys_not_null"}

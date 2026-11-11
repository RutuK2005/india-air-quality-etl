"""End-to-end pipeline tests against a temporary DuckDB file (no network)."""

from datetime import date

import pytest

from aq_etl import warehouse
from aq_etl.pipeline import fixed_ranges, plan_incremental, run_pipeline
from aq_etl.quality import DataQualityError
from tests.conftest import FIXTURE_END, FIXTURE_START, FakeClient


def counts(settings):
    with warehouse.connect(settings.warehouse_path) as con:
        return warehouse.table_counts(con)


def test_full_run_loads_silver_gold_and_audit(settings, fake_client):
    ranges = fixed_ranges(settings, FIXTURE_START, FIXTURE_END)
    result = run_pipeline(settings, ranges, "range", client=fake_client)

    assert result.status == "success"
    # 2 cities x 48 hours; bronze counts weather + air rows separately
    assert result.rows == {"bronze": 192, "silver": 96, "gold": 4}
    assert counts(settings)["silver_hourly"] == 96
    assert counts(settings)["gold_daily_city"] == 4
    with warehouse.connect(settings.warehouse_path) as con:
        run = warehouse.recent_runs(con).iloc[0]
        assert run["status"] == "success" and run["rows_silver"] == 96


def test_rerunning_same_range_is_idempotent(settings):
    ranges = fixed_ranges(settings, FIXTURE_START, FIXTURE_END)
    first = run_pipeline(settings, ranges, "range", client=FakeClient())
    second = run_pipeline(settings, ranges, "range", client=FakeClient())
    # an overlapping range (just day 2) must not duplicate either
    run_pipeline(settings, fixed_ranges(settings, FIXTURE_END, FIXTURE_END), "range", FakeClient())

    c = counts(settings)
    assert c["silver_hourly"] == 96 and c["gold_daily_city"] == 4
    assert c["pipeline_runs"] == 3
    with warehouse.connect(settings.warehouse_path) as con:
        run_ids = {
            r[0] for r in con.execute("SELECT DISTINCT run_id FROM gold_daily_city").fetchall()
        }
    # day 1 rows were last written by run 2, day 2 rows by run 3
    assert first.run_id not in run_ids and second.run_id in run_ids


def test_plan_incremental_uses_watermarks(settings):
    today = date(2026, 11, 3)
    # no watermark -> look back `initial_lookback_days` (7) ending yesterday
    plan = plan_incremental(settings, {}, today)
    assert plan["Delhi"] == (date(2026, 10, 27), date(2026, 11, 2))
    # watermark -> start the day after it; up-to-date city is skipped
    plan = plan_incremental(
        settings, {"Delhi": date(2026, 10, 31), "Mumbai": date(2026, 11, 2)}, today
    )
    assert plan == {"Delhi": (date(2026, 11, 1), date(2026, 11, 2))}


def test_incremental_run_only_fetches_new_days(settings):
    run_pipeline(
        settings, fixed_ranges(settings, FIXTURE_START, FIXTURE_START), "range", FakeClient()
    )
    with warehouse.connect(settings.warehouse_path) as con:
        marks = warehouse.get_watermarks(con)
    assert marks == {"Delhi": FIXTURE_START, "Mumbai": FIXTURE_START}

    client = FakeClient()
    plan = plan_incremental(settings, marks, today=date(2026, 11, 3))
    run_pipeline(settings, plan, "incremental", client=client)
    assert {(start, end) for _, _, start, end in client.calls} == {(FIXTURE_END, FIXTURE_END)}

    with warehouse.connect(settings.warehouse_path) as con:
        assert warehouse.get_watermarks(con) == {"Delhi": FIXTURE_END, "Mumbai": FIXTURE_END}
    assert (
        plan_incremental(settings, {"Delhi": FIXTURE_END, "Mumbai": FIXTURE_END}, date(2026, 11, 3))
        == {}
    )


def test_backfill_never_moves_watermark_backwards(settings):
    run_pipeline(
        settings, fixed_ranges(settings, FIXTURE_START, FIXTURE_END), "range", FakeClient()
    )
    run_pipeline(
        settings, fixed_ranges(settings, FIXTURE_START, FIXTURE_START), "backfill", FakeClient()
    )
    with warehouse.connect(settings.warehouse_path) as con:
        assert set(warehouse.get_watermarks(con).values()) == {FIXTURE_END}


def test_failed_dq_check_blocks_load_and_is_audited(settings):
    def corrupt(source, payload):
        if source == "weather":
            payload["hourly"]["relative_humidity_2m"][5] = 150  # impossible humidity

    ranges = fixed_ranges(settings, FIXTURE_START, FIXTURE_END)
    with pytest.raises(DataQualityError, match="silver.ranges"):
        run_pipeline(settings, ranges, "range", client=FakeClient(mutate=corrupt))

    c = counts(settings)
    assert c["silver_hourly"] == 0 and c["gold_daily_city"] == 0 and c["watermarks"] == 0
    with warehouse.connect(settings.warehouse_path) as con:
        run = warehouse.recent_runs(con).iloc[0]
    assert run["status"] == "failed"
    assert "humidity_pct" in run["error"]


def test_empty_plan_is_skipped_without_touching_warehouse(settings):
    result = run_pipeline(settings, {}, "incremental", client=FakeClient())
    assert result.status == "skipped"
    assert not settings.warehouse_path.exists()

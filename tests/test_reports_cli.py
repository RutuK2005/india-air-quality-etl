from typer.testing import CliRunner

from aq_etl import reports, warehouse
from aq_etl.cli import app
from aq_etl.pipeline import fixed_ranges, run_pipeline
from tests.conftest import FIXTURE_END, FIXTURE_START, FakeClient


def load_fixture_data(settings):
    run_pipeline(
        settings, fixed_ranges(settings, FIXTURE_START, FIXTURE_END), "range", FakeClient()
    )


def test_every_sql_report_runs_and_returns_rows(settings):
    load_fixture_data(settings)
    with warehouse.connect(settings.warehouse_path) as con:
        results = reports.run_reports(con, settings.sql_dir)
    assert len(results) == 6
    for title, df in results:
        assert not df.empty, title


def test_export_writes_parquet_and_csv(settings):
    load_fixture_data(settings)
    with warehouse.connect(settings.warehouse_path) as con:
        paths = reports.export_tables(con, settings.export_dir)
    names = sorted(p.name for p in paths)
    assert names == ["gold_daily_city.csv", "gold_daily_city.parquet", "silver_hourly.parquet"]
    header = (settings.export_dir / "gold_daily_city.csv").read_text().splitlines()[0]
    assert header.startswith("city,date,avg_temp_c")


def test_cli_status_and_report(settings, tmp_path):
    load_fixture_data(settings)
    config = tmp_path / "config.toml"
    config.write_text(
        f"""
[pipeline]
warehouse_path = "{settings.warehouse_path.as_posix()}"
sql_dir = "{settings.sql_dir.as_posix()}"

[[cities]]
name = "Delhi"
latitude = 28.6
longitude = 77.2
""",
        encoding="utf-8",
    )
    runner = CliRunner()
    status = runner.invoke(app, ["--config", str(config), "status"])
    assert status.exit_code == 0, status.output
    assert "silver_hourly" in status.output and "2026-11-02" in status.output

    report = runner.invoke(app, ["--config", str(config), "report", "--name", "worst"])
    assert report.exit_code == 0, report.output
    assert "Worst AQI day per city" in report.output


def test_cli_run_requires_a_mode(tmp_path):
    config = tmp_path / "config.toml"
    config.write_text('[[cities]]\nname = "Delhi"\nlatitude = 1\nlongitude = 1\n', encoding="utf-8")
    result = CliRunner().invoke(app, ["--config", str(config), "run"])
    assert result.exit_code != 0
    args = ["--config", str(config), "run", "--start", "2026-11-02", "--end", "2026-11-01"]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 2 and "before" in result.output

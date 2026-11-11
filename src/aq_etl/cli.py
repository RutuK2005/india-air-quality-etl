"""Command-line interface: `etl run | backfill | report | status | export`."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import typer

from aq_etl import pipeline, reports, warehouse
from aq_etl.config import Settings, load_settings
from aq_etl.quality import DataQualityError

app = typer.Typer(
    help="Incremental ETL for Indian city weather and air quality.", no_args_is_help=True
)
state: dict[str, Settings] = {}


def _settings() -> Settings:
    return state["settings"]


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise typer.BadParameter(f"expected YYYY-MM-DD, got {value!r}") from exc


def _print_table(title: str, df: pd.DataFrame) -> None:
    typer.secho(f"\n{title}", bold=True)
    typer.echo("(no rows)" if df.empty else df.to_string(index=False))


def _execute(ranges: pipeline.DateRanges, mode: str) -> None:
    """Run the pipeline and turn failures into a clean exit code."""
    settings = _settings()
    for city, (start, end) in ranges.items():
        typer.echo(f"  {city:<10} {start} -> {end}")
    try:
        result = pipeline.run_pipeline(settings, ranges, mode)
    except DataQualityError as exc:
        typer.secho("Data-quality gate FAILED, nothing was loaded:", fg="red")
        for failure in exc.failures:
            typer.echo(f"  - {failure.name}: {failure.details}")
        raise typer.Exit(code=1) from exc
    except Exception as exc:
        typer.secho(f"Run failed: {exc}", fg="red")
        raise typer.Exit(code=1) from exc
    if result.status == "skipped":
        typer.echo("Nothing to do - every city is up to date.")
    else:
        typer.secho(f"Run {result.run_id} {result.status}: {result.rows}", fg="green")


@app.callback()
def main(
    config: Path = typer.Option(Path("config.toml"), "--config", "-c", help="Path to config TOML."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging."),
) -> None:
    """Load the config once for every sub-command."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    state["settings"] = load_settings(config)


@app.command()
def run(
    start: str | None = typer.Option(None, help="First date (YYYY-MM-DD)."),
    end: str | None = typer.Option(None, help="Last date (YYYY-MM-DD), inclusive."),
    incremental: bool = typer.Option(
        False, "--incremental", help="Only fetch days after each city's watermark."
    ),
) -> None:
    """Run the pipeline for an explicit date range or incrementally."""
    settings = _settings()
    if incremental:
        if start or end:
            raise typer.BadParameter("use either --incremental or --start/--end, not both")
        with warehouse.connect(settings.warehouse_path) as con:
            marks = warehouse.get_watermarks(con)
        ranges = pipeline.plan_incremental(settings, marks, pipeline.today_local(settings))
        typer.echo("Incremental run plan:")
        _execute(ranges, "incremental")
    elif start and end:
        try:
            ranges = pipeline.fixed_ranges(settings, _parse_date(start), _parse_date(end))
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        typer.echo("Explicit range run:")
        _execute(ranges, "range")
    else:
        raise typer.BadParameter("pass --start and --end, or --incremental")


@app.command()
def backfill(
    days: int = typer.Option(
        30, min=1, help="How many complete days to (re)load, ending yesterday."
    ),
) -> None:
    """Reload the last N days for every city (safe to repeat thanks to upserts)."""
    settings = _settings()
    end = pipeline.today_local(settings) - timedelta(days=1)
    ranges = pipeline.fixed_ranges(settings, end - timedelta(days=days - 1), end)
    typer.echo(f"Backfill of {days} day(s):")
    _execute(ranges, "backfill")


@app.command()
def report(
    name: str | None = typer.Option(None, help="Only run queries whose file name contains this."),
) -> None:
    """Run the analysis queries in sql/ and print the results."""
    settings = _settings()
    with warehouse.connect(settings.warehouse_path) as con:
        results = reports.run_reports(con, settings.sql_dir, name)
    if not results:
        typer.echo(f"No queries found in {settings.sql_dir}")
    for title, df in results:
        _print_table(title, df)


@app.command()
def status() -> None:
    """Show watermarks, table sizes and recent runs."""
    settings = _settings()
    with warehouse.connect(settings.warehouse_path) as con:
        marks = warehouse.get_watermarks(con)
        counts = warehouse.table_counts(con)
        runs = warehouse.recent_runs(con)
    typer.secho(f"Warehouse: {settings.warehouse_path}", bold=True)
    for table, n in counts.items():
        typer.echo(f"  {table:<16} {n:>7} rows")
    marks_df = pd.DataFrame(sorted(marks.items()), columns=["city", "last_loaded_date"])
    _print_table("Watermarks", marks_df)
    _print_table("Recent runs", runs)


@app.command()
def export(
    fmt: str = typer.Option("both", "--format", help="parquet, csv or both."),
) -> None:
    """Export gold (and silver) tables for Power BI / Spark."""
    settings = _settings()
    formats = {"parquet": ("parquet",), "csv": ("csv",), "both": ("parquet", "csv")}
    if fmt not in formats:
        raise typer.BadParameter("--format must be parquet, csv or both")
    with warehouse.connect(settings.warehouse_path) as con:
        paths = reports.export_tables(con, settings.export_dir, formats[fmt])
    for path in paths:
        typer.echo(f"wrote {path}")


if __name__ == "__main__":
    app()

"""Run the analysis queries in `sql/` and export gold data for BI tools."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


def list_queries(sql_dir: Path) -> list[Path]:
    """All `.sql` files in the folder, in file-name order (01_, 02_, ...)."""
    return sorted(sql_dir.glob("*.sql"))


def query_title(path: Path) -> str:
    """Title from a leading `-- Title: ...` comment, else the file name."""
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.lower().startswith("-- title:"):
            return line.split(":", 1)[1].strip()
    return path.stem


def run_query(con: duckdb.DuckDBPyConnection, path: Path) -> pd.DataFrame:
    """Execute one SQL file and return the result as a DataFrame."""
    return con.execute(path.read_text(encoding="utf-8")).df()


def run_reports(
    con: duckdb.DuckDBPyConnection, sql_dir: Path, name: str | None = None
) -> list[tuple[str, pd.DataFrame]]:
    """Run every query (or only those whose file name contains `name`)."""
    paths = [p for p in list_queries(sql_dir) if name is None or name in p.stem]
    return [(query_title(p), run_query(con, p)) for p in paths]


def export_tables(
    con: duckdb.DuckDBPyConnection, export_dir: Path, formats: tuple[str, ...] = ("parquet", "csv")
) -> list[Path]:
    """Write gold (and silver, as Parquet) to files that Power BI or Spark can read."""
    export_dir.mkdir(parents=True, exist_ok=True)
    jobs = [("gold_daily_city", "city, date", fmt) for fmt in formats]
    jobs.append(("silver_hourly", "city, ts_ist", "parquet"))
    written = []
    for table, order_by, fmt in jobs:
        path = export_dir / f"{table}.{fmt}"
        options = "FORMAT parquet" if fmt == "parquet" else "FORMAT csv, HEADER"
        # DuckDB's COPY writes the file directly, no pandas round-trip needed.
        con.execute(
            f"COPY (SELECT * FROM {table} ORDER BY {order_by}) TO '{path.as_posix()}' ({options})"
        )
        written.append(path)
    return written

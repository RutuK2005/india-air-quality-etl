"""Load: DuckDB warehouse with idempotent upserts, watermarks and a run audit table."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS silver_hourly (
    city             VARCHAR   NOT NULL,
    ts_ist           TIMESTAMP NOT NULL,
    ts_utc           TIMESTAMP NOT NULL,
    date_ist         DATE      NOT NULL,
    hour             INTEGER   NOT NULL,
    temperature_c    DOUBLE,
    humidity_pct     DOUBLE,
    precipitation_mm DOUBLE,
    wind_speed_kmh   DOUBLE,
    pm2_5            DOUBLE,
    pm10             DOUBLE,
    no2              DOUBLE,
    o3               DOUBLE,
    aqi_pm25         INTEGER,
    aqi_category     VARCHAR,
    run_id           VARCHAR,
    loaded_at        TIMESTAMP,
    PRIMARY KEY (city, ts_ist)
);

CREATE TABLE IF NOT EXISTS gold_daily_city (
    city             VARCHAR NOT NULL,
    date             DATE    NOT NULL,
    avg_temp_c       DOUBLE,
    min_temp_c       DOUBLE,
    max_temp_c       DOUBLE,
    avg_humidity_pct DOUBLE,
    total_precip_mm  DOUBLE,
    rainy_hours      INTEGER,
    avg_wind_kmh     DOUBLE,
    avg_pm2_5        DOUBLE,
    max_pm2_5        DOUBLE,
    avg_pm10         DOUBLE,
    avg_no2          DOUBLE,
    avg_o3           DOUBLE,
    aqi_pm25         INTEGER,
    aqi_category     VARCHAR,
    hours_observed   INTEGER,
    run_id           VARCHAR,
    loaded_at        TIMESTAMP,
    PRIMARY KEY (city, date)
);

CREATE TABLE IF NOT EXISTS watermarks (
    city             VARCHAR PRIMARY KEY,
    last_loaded_date DATE NOT NULL,
    updated_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id      VARCHAR PRIMARY KEY,
    mode        VARCHAR,
    started_at  TIMESTAMP,
    finished_at TIMESTAMP,
    start_date  DATE,
    end_date    DATE,
    status      VARCHAR,
    rows_bronze INTEGER,
    rows_silver INTEGER,
    rows_gold   INTEGER,
    error       VARCHAR
);
"""


def connect(path: Path | str) -> duckdb.DuckDBPyConnection:
    """Open (or create) the warehouse file and make sure all tables exist."""
    if str(path) != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute(SCHEMA_SQL)
    return con


def upsert(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame, run_id: str) -> int:
    """Insert rows, replacing any existing row with the same primary key.

    The frame is first copied into a temporary staging table, then merged with
    `INSERT OR REPLACE`. Loading the same data twice therefore leaves exactly one
    row per key: re-runs and overlapping backfills never create duplicates.
    """
    staging = df.assign(run_id=run_id, loaded_at=datetime.now())
    con.register("staging_df", staging)
    try:
        con.execute("CREATE OR REPLACE TEMP TABLE staging AS SELECT * FROM staging_df")
        con.execute(f"INSERT OR REPLACE INTO {table} BY NAME SELECT * FROM staging")
    finally:
        con.unregister("staging_df")
        con.execute("DROP TABLE IF EXISTS staging")
    return len(df)


def update_watermarks(con: duckdb.DuckDBPyConnection, loaded: dict[str, date]) -> None:
    """Move each city's watermark forward to the last date just loaded.

    `greatest()` keeps watermarks monotonic: back-filling an old range never
    moves a watermark backwards and causes recent days to be fetched again.
    """
    for city, last_date in loaded.items():
        con.execute(
            """
            INSERT INTO watermarks VALUES (?, ?, now())
            ON CONFLICT (city) DO UPDATE SET
                last_loaded_date = greatest(watermarks.last_loaded_date, excluded.last_loaded_date),
                updated_at = excluded.updated_at
            """,
            [city, last_date],
        )


def load_layers(
    con: duckdb.DuckDBPyConnection,
    silver: pd.DataFrame,
    gold: pd.DataFrame,
    run_id: str,
    loaded: dict[str, date],
) -> None:
    """Load silver + gold and advance watermarks in ONE transaction (all or nothing)."""
    con.begin()
    try:
        upsert(con, "silver_hourly", silver, run_id)
        upsert(con, "gold_daily_city", gold, run_id)
        update_watermarks(con, loaded)
        con.commit()
    except Exception:
        con.rollback()
        raise


def get_watermarks(con: duckdb.DuckDBPyConnection) -> dict[str, date]:
    """Return {city: last successfully loaded date}."""
    rows = con.execute("SELECT city, last_loaded_date FROM watermarks").fetchall()
    return dict(rows)


def start_run(
    con: duckdb.DuckDBPyConnection, run_id: str, mode: str, start: date, end: date
) -> None:
    """Record a new run with status 'running'."""
    con.execute(
        "INSERT INTO pipeline_runs (run_id, mode, started_at, start_date, end_date, status) "
        "VALUES (?, ?, now(), ?, ?, 'running')",
        [run_id, mode, start, end],
    )


def finish_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    status: str,
    rows: dict[str, int],
    error: str | None = None,
) -> None:
    """Close a run with its final status, per-stage row counts and any error."""
    con.execute(
        """
        UPDATE pipeline_runs
        SET finished_at = now(), status = ?, rows_bronze = ?, rows_silver = ?,
            rows_gold = ?, error = ?
        WHERE run_id = ?
        """,
        [
            status,
            rows.get("bronze", 0),
            rows.get("silver", 0),
            rows.get("gold", 0),
            error,
            run_id,
        ],
    )


def recent_runs(con: duckdb.DuckDBPyConnection, limit: int = 10) -> pd.DataFrame:
    """Most recent pipeline runs, newest first."""
    return con.execute(
        """
        SELECT run_id, mode, start_date, end_date, status, rows_bronze, rows_silver,
               rows_gold, date_diff('second', started_at, finished_at) AS seconds, error
        FROM pipeline_runs ORDER BY started_at DESC LIMIT ?
        """,
        [limit],
    ).df()


def table_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Row counts for the main warehouse tables."""
    tables = ["silver_hourly", "gold_daily_city", "watermarks", "pipeline_runs"]
    return {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in tables}

# Walkthrough: how this pipeline works

A study guide for explaining this project in an interview. Read it top to bottom once,
then use the "Trace one run" section and the Q&A to rehearse.

## 1. The 30-second pitch

"I built an incremental ETL pipeline that collects hourly weather and air-quality data for
eight Indian cities from the Open-Meteo APIs. Raw JSON is landed as a bronze layer, pandas
cleans and joins it into an hourly silver table and a daily gold table with CPCB AQI
categories, a data-quality gate blocks bad data, and DuckDB stores it with idempotent
upserts and watermarks so the pipeline can be re-run safely and only fetches new days.
A CLI runs it, prints SQL reports and exports Parquet for Power BI, and GitHub Actions runs
lint and tests on every push."

## 2. Trace one run

`etl run --start 2026-11-08 --end 2026-11-10` does this:

1. **`cli.py`** parses the options, loads `config.toml` into a `Settings` object
   (`config.py`) and builds a plan `{city: (start, end)}` with `pipeline.fixed_ranges`.
2. **`pipeline.run_pipeline`** creates a `run_id`, opens DuckDB (`warehouse.connect`, which
   also creates the tables) and inserts a `pipeline_runs` row with status `running`.
3. **Extract** (`extract.extract_city`): for each city and each source (weather, air
   quality) the date range is split into chunks (`chunk_dates`), each chunk is fetched by
   `OpenMeteoClient.fetch`, and `land_raw` writes one JSON file per day to
   `data/raw/<source>/<city>/<date>.json`.
4. **Transform** (`pipeline._transform` + `transform.py`): bronze files are read back,
   `flatten_hourly` turns parallel arrays into rows, `build_silver` outer-joins weather
   and air quality on `(city, time)`, adds IST/UTC timestamps and hourly AQI, and
   `build_gold` groups by city and day.
5. **Data quality** (`quality.py`): silver and gold checks run; `enforce` raises
   `DataQualityError` if any failed, so the next step never happens.
6. **Load** (`warehouse.load_layers`): in one transaction, silver and gold are upserted and
   the watermarks are moved forward. Any error rolls everything back.
7. The `finally` block updates the `pipeline_runs` row with `success` or `failed`, row
   counts per stage and the error message.

## 3. Module by module

| Module         | Responsibility                                       | Key functions                                         |
|----------------|------------------------------------------------------|-------------------------------------------------------|
| `config.py`    | Read TOML, typed settings                            | `City`, `Settings`, `load_settings`                   |
| `extract.py`   | HTTP, retries, rate limit, chunking, bronze files    | `build_session`, `OpenMeteoClient`, `chunk_dates`, `split_by_day`, `land_raw`, `read_bronze` |
| `transform.py` | Tidy rows, types, time zones, join, aggregates       | `flatten_hourly`, `add_time_columns`, `build_silver`, `build_gold` |
| `aqi.py`       | CPCB PM2.5 category and sub-index                    | `pm25_category`, `pm25_sub_index`, `categorise_series` |
| `quality.py`   | DQ checks and the gate                               | `check_*`, `run_silver_checks`, `enforce`             |
| `warehouse.py` | DuckDB schema, upsert, watermarks, audit             | `connect`, `upsert`, `load_layers`, `update_watermarks` |
| `pipeline.py`  | Orchestration and incremental planning               | `plan_incremental`, `run_pipeline`                    |
| `reports.py`   | Run `sql/*.sql`, export Parquet/CSV                  | `run_reports`, `export_tables`                        |
| `cli.py`       | Typer commands                                       | `run`, `backfill`, `report`, `status`, `export`       |

Details worth knowing:

- **`build_session`** mounts an `HTTPAdapter` with a urllib3 `Retry`: up to 5 retries on
  connection errors and HTTP 429/500/502/503/504, exponential backoff (in urllib3 2.x the
  first retry is immediate, then `backoff_factor * 2^(n-1)` seconds: 2s, 4s, 8s, ...),
  and it honours a `Retry-After` header.
  Every request also has a timeout, so a hung server cannot block the job forever.
- **`_throttle`** keeps at least `min_request_interval_s` between calls (polite rate
  limiting, separate from retries: one prevents 429s, the other recovers from them).
- **`weather_url_for`** uses the archive API for dates older than ~6 days and the forecast
  API for recent ones, because the ERA5 archive lags real time.
- **`Fetcher` protocol**: the pipeline accepts anything with a `fetch()` method, which is
  how the tests swap in `FakeClient` (dependency injection instead of mocking `requests`).
- **`add_time_columns`**: the API returns local wall-clock strings; `tz_localize` attaches
  Asia/Kolkata and `tz_convert("UTC")` gives UTC. Both are stored naive.
- **`upsert`**: register the DataFrame, copy to a temp `staging` table, then
  `INSERT OR REPLACE INTO <table> BY NAME SELECT * FROM staging`. The primary key decides
  what "the same row" means.

## 4. Key concepts

**ETL vs ELT.** ETL transforms data *before* loading it into the warehouse (here: pandas
builds silver/gold, then DuckDB stores them). ELT loads raw data first and transforms
inside the warehouse with SQL (the dbt style), which suits cloud warehouses with cheap
compute. This project is ETL with an ELT flavour at the end: the analysis queries in `sql/`
run inside DuckDB.

**Medallion layers.** *Bronze* = raw data exactly as received (here, JSON files), kept so
you can reprocess without calling the source again. *Silver* = cleaned, typed, deduplicated,
joined data at the original grain (one row per city per hour). *Gold* = business-level
aggregates ready for dashboards (one row per city per day).

**Idempotency.** Running the same job twice gives the same result as running it once. It
matters because jobs get retried after failures and backfills overlap with earlier loads.
Achieved with natural primary keys plus upserts (and overwriting bronze files by key).
Verified by a test that loads the same range three times and checks the row counts.

**Incremental loads and watermarks.** A watermark is the "high-water mark" of what has been
loaded: here, the last date per city. An incremental run fetches only `(watermark, yesterday]`.
Rules that make it safe: only move it after a successful load, in the same transaction as
the data, and never move it backwards (`greatest()`). Yesterday, not today, is the end,
because today is not complete yet.

**Backfill.** Deliberately reloading a historical range (new city added, bug fixed, source
revised data). Safe here because loads are idempotent and watermarks are monotonic.

**Data quality.** Checks between transform and load: schema (columns and types), completeness
(no null keys, limited null share in measures), validity (physical ranges), uniqueness
(one row per key). Failing fast before load keeps bad data out of dashboards; the failure is
recorded in `pipeline_runs` for debugging.

**Retries with backoff.** Transient failures (timeouts, 503, 429 rate limits) often succeed
on a second try. Exponential backoff waits longer each time so a struggling server is not
hammered. Only retry idempotent requests (GET) and only for transient status codes; a 400
means the request itself is wrong and retrying will not help.

**Transactions.** `load_layers` wraps silver, gold and watermark updates in
`BEGIN ... COMMIT`; any exception triggers `ROLLBACK`, so the warehouse is never half-loaded.

**Observability.** `pipeline_runs` is a minimal audit log: who ran what range, when, how many
rows per stage, and why it failed. `etl status` reads it.

## 5. Likely interview questions

1. **What happens if you run the same date range twice?**
   Nothing is duplicated. Bronze files are overwritten by source/city/date, and silver/gold
   rows are upserted on their primary keys `(city, ts_ist)` and `(city, date)` with
   `INSERT OR REPLACE`. The test `test_rerunning_same_range_is_idempotent` proves it, and
   the live run shows 576 silver rows after three overlapping loads.

2. **How does the incremental mode know what to fetch?**
   The `watermarks` table stores the last loaded date per city. `plan_incremental` starts at
   watermark + 1 and ends at yesterday; a city with no watermark looks back
   `initial_lookback_days`. Up-to-date cities are skipped.

3. **What if the pipeline crashes halfway through loading?**
   Silver, gold and watermark updates are in one DuckDB transaction, so they roll back
   together. The watermark is not advanced, so the next incremental run fetches the same
   days again; idempotent upserts make that harmless. The run is marked `failed` with the
   error in `pipeline_runs`.

4. **How do you handle API failures and rate limits?**
   A `requests.Session` with a urllib3 `Retry` (5 retries, exponential backoff, on
   429/5xx and connection errors, honouring `Retry-After`), timeouts on every request, and a
   minimum interval between requests. Long ranges are chunked to keep responses small.

5. **What data-quality checks do you run, and what happens when one fails?**
   Not-empty, schema, non-null keys, null share of measures under 10 %, physical ranges
   (humidity 0-100, PM2.5 0-2000, ...), and uniqueness of `(city, ts_ist)` and
   `(city, date)`. A failure raises `DataQualityError` before the load, so nothing is
   written, watermarks stay put, and the CLI prints the failing checks and exits with code 1.

6. **Why DuckDB and not Postgres or a cloud warehouse?**
   It is an embedded, columnar analytical database: zero setup, a single file, very fast
   aggregations, direct pandas and Parquet integration, and it still has primary keys and
   transactions. For one batch writer and analytical reads it is ideal. With many concurrent
   writers or a team sharing data I would move to Postgres or BigQuery/Snowflake; the SQL
   and the upsert pattern carry over.

7. **How do you handle time zones?**
   The API is asked for Asia/Kolkata local time so "a day" is an IST day. In pandas the
   naive local timestamps are localised to Asia/Kolkata and converted to UTC; both are
   stored. India has no DST, so there are no ambiguous or missing hours.

8. **Why an outer join between weather and air quality?**
   An inner join would silently drop hours missing from one source. The outer join keeps
   them with NaNs, and the DQ null-share check decides whether the gap is acceptable, so
   problems are visible instead of hidden.

9. **How is the AQI category calculated?**
   From CPCB PM2.5 breakpoints: Good 0-30, Satisfactory 31-60, Moderate 61-90, Poor
   91-120, Very Poor 121-250, Severe above 250 ug/m3; the sub-index is linear interpolation
   inside each band. CPCB defines AQI on a 24-hour average, so the gold (daily) category is
   the meaningful one; the hourly category is indicative. It is a PM2.5 sub-index only, not
   the full multi-pollutant AQI.

10. **How would you scale this to every district in India for ten years?**
    Partition bronze/silver as Parquet by date and city, process in parallel per partition,
    move the gold aggregation to Spark (see `spark/gold_daily_spark.py`) or push it into
    the warehouse as SQL/dbt models, orchestrate with Airflow, and use a cloud warehouse.
    The concepts (bronze/silver/gold, upserts, watermarks, DQ gates) stay the same.

11. **How do you test a pipeline that depends on an external API?**
    Record small real responses once (`scripts/record_fixtures.py`) and replay them with a
    `FakeClient` that implements the same `fetch()` interface. Tests run offline and
    deterministically against a temporary DuckDB file.

12. **What would you improve next?**
    Re-pull the last few days on each incremental run because recent model data gets
    revised, alerting on DQ failures, an orchestrator, dbt for SQL models, and comparing
    the modelled values with ground-station data.

## 6. Things to be honest about

- The air-quality numbers are modelled (CAMS) grid values, not CPCB station readings.
- Recent days come from the forecast API and may change later; a backfill refreshes them.
- One failing city fails the whole run (simplicity over partial success).
- The Spark job is a reference example; the main pipeline does not use it.

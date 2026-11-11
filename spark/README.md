# Optional Spark backend

`gold_daily_spark.py` re-implements the gold step (hourly silver -> daily city
metrics) in PySpark. It is an **alternative backend for illustration**, not part
of the pipeline: the main pipeline uses pandas + DuckDB, and the tests do not
need Spark or Java.

Why include it? The pandas version is the right tool for ~8 cities of hourly
data (thousands of rows). If the city list grew to every district in India for
several years, the same `groupBy` + `agg` logic moves to Spark almost one-to-one.

```bash
pip install -e ".[spark]"     # PySpark; requires Java 17+ on PATH
etl export                    # produces data/exports/silver_hourly.parquet
python spark/gold_daily_spark.py data/exports/silver_hourly.parquet data/exports/gold_spark
```

Differences from the pandas gold table: it skips a few columns (wind, PM10,
NO2, O3, sub-index) to keep the example short, and writes Parquet partitioned
by city.

Note: this script was not executed while building the project (no Java/PySpark
in the build environment); it is a reference example.

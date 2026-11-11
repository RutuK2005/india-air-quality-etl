"""Optional: the gold daily aggregation written in PySpark.

This is an *alternative backend* to show how the pandas gold step would look on
Spark when data outgrows one machine. It is not used by the pipeline or tests.

    pip install -e ".[spark]"          # needs Java 17+
    etl export                         # writes data/exports/silver_hourly.parquet
    python spark/gold_daily_spark.py data/exports/silver_hourly.parquet data/exports/gold_spark
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def pm25_category(col: F.Column) -> F.Column:
    """CPCB PM2.5 categories (same breakpoints as aq_etl.aqi)."""
    return (
        F.when(col.isNull(), None)
        .when(col <= 30, "Good")
        .when(col <= 60, "Satisfactory")
        .when(col <= 90, "Moderate")
        .when(col <= 120, "Poor")
        .when(col <= 250, "Very Poor")
        .otherwise("Severe")
    )


def main(silver_path: str, output_path: str) -> None:
    spark = SparkSession.builder.appName("gold_daily_city").getOrCreate()
    silver = spark.read.parquet(silver_path)

    gold = (
        silver.groupBy("city", F.col("date_ist").alias("date"))
        .agg(
            F.round(F.avg("temperature_c"), 2).alias("avg_temp_c"),
            F.min("temperature_c").alias("min_temp_c"),
            F.max("temperature_c").alias("max_temp_c"),
            F.round(F.avg("humidity_pct"), 2).alias("avg_humidity_pct"),
            F.round(F.sum("precipitation_mm"), 2).alias("total_precip_mm"),
            F.sum((F.col("precipitation_mm") > 0.1).cast("int")).alias("rainy_hours"),
            F.round(F.avg("pm2_5"), 2).alias("avg_pm2_5"),
            F.max("pm2_5").alias("max_pm2_5"),
            F.count("pm2_5").alias("hours_observed"),
        )
        .withColumn("aqi_category", pm25_category(F.col("avg_pm2_5")))
        .orderBy("city", "date")
    )

    gold.show(10, truncate=False)
    # partitionBy writes one folder per city, a common layout for large tables
    gold.write.mode("overwrite").partitionBy("city").parquet(output_path)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: gold_daily_spark.py <silver_parquet> <output_dir>")
    main(sys.argv[1], sys.argv[2])

-- Title: Worst AQI day per city
-- Window function: rank each city's days by daily PM2.5 sub-index and keep the top one.
SELECT city, date, aqi_pm25, aqi_category, avg_pm2_5, total_precip_mm
FROM (
    SELECT
        *,
        row_number() OVER (PARTITION BY city ORDER BY aqi_pm25 DESC, date) AS rnk
    FROM gold_daily_city
)
WHERE rnk = 1
ORDER BY aqi_pm25 DESC;

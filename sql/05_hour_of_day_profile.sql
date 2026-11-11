-- Title: Most and least polluted hour of day per city (IST)
-- arg_max / arg_min return the hour at which the average PM2.5 peaks / bottoms out.
WITH by_hour AS (
    SELECT city, hour, avg(pm2_5) AS avg_pm2_5
    FROM silver_hourly
    GROUP BY city, hour
)
SELECT
    city,
    arg_max(hour, avg_pm2_5)     AS worst_hour,
    round(max(avg_pm2_5), 1)     AS worst_hour_pm2_5,
    arg_min(hour, avg_pm2_5)     AS cleanest_hour,
    round(min(avg_pm2_5), 1)     AS cleanest_hour_pm2_5
FROM by_hour
GROUP BY city
ORDER BY worst_hour_pm2_5 DESC;

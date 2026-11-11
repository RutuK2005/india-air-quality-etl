-- Title: PM2.5 by day of week (all cities)
-- Weekday vs weekend traffic patterns; needs a few weeks of data to be meaningful.
SELECT
    isodow(date)             AS dow,
    dayname(date)            AS weekday,
    count(*)                 AS city_days,
    round(avg(avg_pm2_5), 1) AS avg_pm2_5,
    round(avg(avg_no2), 1)   AS avg_no2
FROM gold_daily_city
GROUP BY ALL
ORDER BY dow;

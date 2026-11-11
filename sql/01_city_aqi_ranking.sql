-- Title: City ranking by average PM2.5 (worst first)
-- Share of days whose 24h-mean PM2.5 is 'Poor' or worse on the CPCB scale.
SELECT
    city,
    count(*)                                       AS days,
    round(avg(avg_pm2_5), 1)                       AS avg_pm2_5,
    round(max(max_pm2_5), 1)                       AS peak_hourly_pm2_5,
    round(100.0 * count(*) FILTER (
        WHERE aqi_category IN ('Poor', 'Very Poor', 'Severe')) / count(*), 0) AS pct_poor_or_worse
FROM gold_daily_city
GROUP BY city
ORDER BY avg_pm2_5 DESC;

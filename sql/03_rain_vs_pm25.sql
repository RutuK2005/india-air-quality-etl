-- Title: Rain vs PM2.5 (hourly)
-- Does rain wash particles out of the air? Compare wet and dry hours per city.
-- corr() is NULL when a city had no rain at all in the loaded range.
SELECT
    city,
    count(*) FILTER (WHERE precipitation_mm > 0.1)                  AS rainy_hours,
    round(avg(pm2_5) FILTER (WHERE precipitation_mm > 0.1), 1)      AS pm2_5_rainy,
    round(avg(pm2_5) FILTER (WHERE precipitation_mm <= 0.1), 1)     AS pm2_5_dry,
    round(corr(precipitation_mm, pm2_5), 2)                         AS corr_rain_pm2_5,
    round(corr(wind_speed_kmh, pm2_5), 2)                           AS corr_wind_pm2_5
FROM silver_hourly
GROUP BY city
ORDER BY city;

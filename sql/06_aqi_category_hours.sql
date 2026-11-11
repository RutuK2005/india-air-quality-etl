-- Title: Hours spent in each CPCB category (hourly, indicative)
-- PIVOT turns category rows into columns: one row per city.
PIVOT (
    SELECT city, aqi_category FROM silver_hourly WHERE aqi_category IS NOT NULL
)
ON aqi_category IN ('Good', 'Satisfactory', 'Moderate', 'Poor', 'Very Poor', 'Severe')
USING count(*)
GROUP BY city
ORDER BY city;

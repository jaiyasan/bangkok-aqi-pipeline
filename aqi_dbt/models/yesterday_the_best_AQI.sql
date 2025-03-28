-- What time yesterday had the best AQI?
SELECT timestamp, aqi
FROM air_quality_data
WHERE timestamp::date = CURRENT_DATE - INTERVAL '1 day'
ORDER BY aqi ASC
LIMIT 1
-- What is the best air quality this week (lowest AQI)?
WITH weekly_aqi AS (
    SELECT
        city,
        state,
        country,
        aqi,
        timestamp::date AS date  -- Convert timestamp to date
    FROM air_quality_data
    WHERE timestamp >= current_date - interval '7 days'  -- Data from the last 7 days
),
lowest_aqi_data AS (
    SELECT
        city,
        state,
        country,
        date,
        aqi
    FROM weekly_aqi
    WHERE (city, state, country, aqi) IN (
        SELECT
            city,
            state,
            country,
            MIN(aqi)  -- Find the minimum AQI for each city
        FROM weekly_aqi
        GROUP BY city, state, country
    )
)
SELECT
    city,
    state,
    country,
    date,
    TO_CHAR(date, 'Day') AS day_of_week,  -- Extract day of the week
    aqi AS lowest_aqi  -- Find the lowest AQI for the city on a specific date
FROM lowest_aqi_data
ORDER BY aqi ASC  -- Order to get the minimum AQI first
LIMIT 1  -- Get the row with the lowest AQI
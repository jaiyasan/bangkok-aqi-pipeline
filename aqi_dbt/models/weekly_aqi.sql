-- What is the average AQI value for the past 7 days?
WITH day_of_week_aqi AS (
    SELECT
        EXTRACT(DOW FROM timestamp) AS day_of_week,  -- Extract day of week (0=Sunday, 6=Saturday)
        AVG(aqi) AS average_aqi  -- Calculate average AQI for each day
    FROM air_quality_data  -- Reference the 'air_quality_data' table
    GROUP BY EXTRACT(DOW FROM timestamp)  -- Group by day of week
)

SELECT
    CASE
        WHEN day_of_week = 0 THEN 'Sunday'
        WHEN day_of_week = 1 THEN 'Monday'
        WHEN day_of_week = 2 THEN 'Tuesday'
        WHEN day_of_week = 3 THEN 'Wednesday'
        WHEN day_of_week = 4 THEN 'Thursday'
        WHEN day_of_week = 5 THEN 'Friday'
        WHEN day_of_week = 6 THEN 'Saturday'
    END AS day_name,
    average_aqi
FROM day_of_week_aqi
ORDER BY day_of_week ASC -- Order by day_of_week from Sunday to Saturday
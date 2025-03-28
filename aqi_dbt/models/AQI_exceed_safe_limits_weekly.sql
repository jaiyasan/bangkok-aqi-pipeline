-- How often does the AQI exceed safe limits (e.g., above 100) this week?
WITH weekly_aqi AS (
    SELECT
        city,
        state,
        country,
        aqi,
        timestamp::date AS date  -- แปลง timestamp เป็นวันที่
    FROM air_quality_data
    WHERE timestamp >= current_date - interval '7 days'  -- เลือกข้อมูลจาก 7 วันที่ผ่านมา (สัปดาห์นี้)
)
SELECT
    COUNT(DISTINCT date) AS count_above_100_days  -- นับจำนวนวันที่ AQI เกิน 100 โดยไม่ซ้ำ
FROM weekly_aqi
WHERE aqi > 100  -- กรองข้อมูลที่ AQI มากกว่า 100
-- วันไหนของสัปดาห์ที่มีค่า AQI สูงที่สุด? Which day of the week generally has the highest AQI values?
WITH day_of_week_aqi AS (
    SELECT
        EXTRACT(DOW FROM timestamp) AS day_of_week, -- ใช้ 'DOW' แทน 'DAYOFWEEK' เพื่อดึงวันของสัปดาห์
        AVG(aqi) AS average_aqi -- คำนวณค่าเฉลี่ยของ AQI สำหรับแต่ละวัน
    FROM {{ ref('air_quality_data') }}  -- อ้างอิงตาราง 'air_quality_data'
    GROUP BY EXTRACT(DOW FROM timestamp)  -- กลุ่มตามวันของสัปดาห์
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
ORDER BY average_aqi DESC
LIMIT 1
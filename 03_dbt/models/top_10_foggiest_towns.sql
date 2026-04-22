WITH c AS (
    SELECT * FROM {{ source('raw_weather_data', 'capitals') }}
),

s AS (
    SELECT * FROM {{ source('raw_weather_data', 'stations') }}
),

wd AS (
    SELECT * FROM {{ source('raw_weather_data', 'weather_data') }}
)

SELECT
    c.capital,

    CAST(AVG(wd.rhum) AS INT64) AS avg_rhum,
    
    COUNT(DISTINCT CASE
        WHEN wd.rhum >= 95
         AND COALESCE(wd.prcp, 0) <= 1
         AND COALESCE(wd.wspd, 999) < 5
        THEN DATE(wd.time)
    END) AS foggy_days,

    SUM(wd.prcp) AS total_precipitation,

    COUNT(DISTINCT wd.time) AS total_days,

    ROUND(
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE
                WHEN wd.rhum >= 95
                AND COALESCE(wd.prcp, 0) <= 1
                AND COALESCE(wd.wspd, 999) < 5
                THEN DATE(wd.time)
            END),
            COUNT(DISTINCT wd.time)
        ) * 100,
        1
    ) AS foggy_days_percentage

FROM c
JOIN s ON c.id = s.capital_id
JOIN wd ON s.id = wd.station

WHERE wd.rhum IS NOT NULL

GROUP BY c.capital

ORDER BY foggy_days_percentage DESC
LIMIT 10
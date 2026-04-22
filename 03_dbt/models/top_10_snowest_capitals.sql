WITH c AS (
    SELECT * 
    FROM {{ source('raw_weather_data', 'capitals') }}
),

s AS (
    SELECT * 
    FROM {{ source('raw_weather_data', 'stations') }}
),

wd AS (
    SELECT * 
    FROM {{ source('raw_weather_data', 'weather_data') }}
),

snow_days AS (
    SELECT
        c.capital,
        DATE(wd.time) AS date
    FROM c
    JOIN s ON c.id = s.capital_id
    JOIN wd ON s.id = wd.station
    WHERE wd.snwd IS NOT NULL
      AND wd.snwd > 0
)

SELECT
    capital,
    COUNT(DISTINCT date) AS snow_days
FROM snow_days
GROUP BY capital
ORDER BY snow_days DESC
LIMIT 10
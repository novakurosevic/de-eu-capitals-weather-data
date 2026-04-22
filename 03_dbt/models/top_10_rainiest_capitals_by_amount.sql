{{ config(
    materialized='view'
) }}

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
    sum(wd.prcp) as total_precipitation,
    count(DISTINCT wd.time) as total_days
FROM c
INNER JOIN s
    ON c.id = s.capital_id
INNER JOIN wd
    ON s.id = wd.station
WHERE
    wd.prcp IS NOT NULL
    AND wd.prcp > 0
GROUP BY
    c.capital
ORDER BY
    total_precipitation DESC
LIMIT 10
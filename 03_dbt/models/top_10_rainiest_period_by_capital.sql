WITH c AS (
    SELECT * FROM {{ source('raw_weather_data', 'capitals') }}
),

s AS (
    SELECT * FROM {{ source('raw_weather_data', 'stations') }}
),

wd AS (
    SELECT * FROM {{ source('raw_weather_data', 'weather_data') }}
),

rainy_days AS (
    SELECT
        c.capital,
        wd.time
    FROM c
    JOIN s ON c.id = s.capital_id
    JOIN wd ON s.id = wd.station
    WHERE wd.prcp > 0
),

grouped AS (
    SELECT
        capital,
        time,
        DATE_SUB(time, INTERVAL ROW_NUMBER() OVER (
            PARTITION BY capital ORDER BY time
        ) DAY) AS grp
    FROM rainy_days
),

streaks AS (
    SELECT
        capital,
        MIN(time) AS start_date,
        MAX(time) AS end_date,
        COUNT(*) AS duration_days
    FROM grouped
    GROUP BY capital, grp
)

SELECT
    capital,
    start_date,
    end_date,
    duration_days
FROM streaks
ORDER BY duration_days DESC
LIMIT 10
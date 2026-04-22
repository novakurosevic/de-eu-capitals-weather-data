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
),

base AS (
    SELECT
        capital,
        date,
        ROW_NUMBER() OVER (
            PARTITION BY capital ORDER BY date
        ) AS rn,
        DATE_SUB(
            date,
            INTERVAL ROW_NUMBER() OVER (
                PARTITION BY capital ORDER BY date
            ) DAY
        ) AS grp
    FROM snow_days
),

streaks AS (
    SELECT
        capital,
        MIN(date) AS start_date,
        MAX(date) AS end_date,
        COUNT(*) AS duration_days
    FROM base
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
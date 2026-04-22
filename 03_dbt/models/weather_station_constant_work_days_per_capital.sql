WITH base AS (

    SELECT 
        station,
        DATE(time) AS date,

        ROW_NUMBER() OVER (
            PARTITION BY station ORDER BY DATE(time)
        ) AS rn,

        DATE_SUB(
            DATE(time),
            INTERVAL ROW_NUMBER() OVER (
                PARTITION BY station ORDER BY DATE(time)
            ) DAY
        ) AS grp

    FROM {{ source('raw_weather_data', 'weather_data') }}

),

streaks AS (

    SELECT
        station,
        grp,
        MIN(date) AS start_date,
        MAX(date) AS end_date,
        COUNT(*) AS duration_days

    FROM base
    GROUP BY station, grp

),

last_streak AS (

    SELECT *
    FROM streaks
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY station ORDER BY end_date DESC
    ) = 1

)

SELECT
    c.capital,
    ls.start_date,
    ls.end_date,
    ls.duration_days AS constantly_work_days

FROM last_streak ls

INNER JOIN {{ source('raw_weather_data', 'stations') }} s
    ON s.id = ls.station

INNER JOIN {{ source('raw_weather_data', 'capitals') }} c
    ON c.id = s.capital_id

ORDER BY ls.duration_days DESC
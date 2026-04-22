{{ config(
    materialized='view'
) }}

WITH cap AS (
    SELECT * FROM {{ source('raw_weather_data', 'capitals') }}
),

st AS (
    SELECT * FROM {{ source('raw_weather_data', 'stations') }}
),

wd AS (
    SELECT * FROM {{ source('raw_weather_data', 'weather_data') }}
)

SELECT 
    wd.time AS weather_date,
    wd.tmax, 
    cap.capital
FROM cap
INNER JOIN st 
    ON cap.id = st.capital_id
INNER JOIN wd 
    ON st.id = wd.station
ORDER BY wd.tmax DESC
LIMIT 10
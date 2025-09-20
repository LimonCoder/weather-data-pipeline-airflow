with source_data as (
    select * from {{ source('weather_dwh','clean_weather_data') }}
)
SELECT
    location,
    ROUND(AVG(temperature)::numeric, 2) AS avg_temp,
    ROUND(AVG(humidity)::numeric, 2) AS avg_humidity,
    ROUND(MAX(temperature)::numeric, 2) AS max_temp,
    ROUND(MIN(temperature)::numeric, 2) AS min_temp,
    CURRENT_TIMESTAMP AS created_at
FROM
    source_data
GROUP BY
    location

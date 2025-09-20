{% snapshot weather_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='location',
        strategy='check',
        check_cols=['avg_temp','avg_humidity','max_temp','min_temp'],
        updated_at='created_at'
    )
}}

with source_data as (
    select * from {{ source('weather_dwh','clean_weather_data') }}
)
select
    location,
    ROUND(AVG(temperature)::numeric, 2) AS avg_temp,
    ROUND(AVG(humidity)::numeric, 2) AS avg_humidity,
    ROUND(MAX(temperature)::numeric, 2) AS max_temp,
    ROUND(MIN(temperature)::numeric, 2) AS min_temp,
    CURRENT_TIMESTAMP AS created_at
from
    source_data
group by
    location

{% endsnapshot %}

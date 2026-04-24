with source as (
    select * from {{ source('raw', 'weather_etl_multiple_cities') }}
),

cleaned as (
    select
        date as weather_date,
        city,
        latitude,
        longitude,
        temp_max,
        temp_min,
        temp_mean,
        cast(weather_code as integer) as weather_code
    from source
)

select * from cleaned
with source as (
    select * from {{ source('forecast', 'weather_forecast_lab1') }}
),

cleaned as (
    select
        cast(ts as date)    as weather_date,
        series              as city,
        forecast            as forecast_temp_max,
        lower_bound         as forecast_lower_bound,
        upper_bound         as forecast_upper_bound
    from source
)

select * from cleaned
with history as (
    select
        weather_date,
        city,
        temp_max              as actual_temp_max,
        temp_min              as actual_temp_min,
        temp_mean             as actual_temp_mean,
        weather_code,
        cast(null as float)   as forecast_temp_max,
        cast(null as float)   as forecast_lower_bound,
        cast(null as float)   as forecast_upper_bound,
        'history'             as record_type
    from {{ ref('stg_weather_history') }}
),

forecast as (
    select
        weather_date,
        city,
        cast(null as float)   as actual_temp_max,
        cast(null as float)   as actual_temp_min,
        cast(null as float)   as actual_temp_mean,
        cast(null as integer) as weather_code,
        forecast_temp_max,
        forecast_lower_bound,
        forecast_upper_bound,
        'forecast'            as record_type
    from {{ ref('stg_weather_forecast') }}
),

unioned as (
    select * from history
    union all
    select * from forecast
)

select * from unioned
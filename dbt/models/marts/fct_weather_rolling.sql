WITH stg_weather_history AS (
    SELECT * FROM {{ ref('stg_weather_history') }}
)

select
    city,
    weather_date,
    temp_max,
    temp_min,
    temp_mean,

    avg(temp_mean) over (
        partition by city
        order by weather_date
        rows between 6 preceding and current row
    ) as temp_mean_7day_avg,

    max(temp_max) over (
        partition by city
        order by weather_date
        rows between 6 preceding and current row
    ) as temp_max_7day,

    min(temp_min) over (
        partition by city
        order by weather_date
        rows between 6 preceding and current row
    ) as temp_min_7day,

    count(*) over (
        partition by city
        order by weather_date
        rows between 6 preceding and current row
    ) as days_in_window

from stg_weather_history
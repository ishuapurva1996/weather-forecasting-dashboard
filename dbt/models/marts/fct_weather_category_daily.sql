with daily as (
    select
        weather_date,
        city,
        weather_code,
        actual_temp_max,
        actual_temp_min,
        actual_temp_mean
    from {{ ref('fct_daily_weather') }}
    where record_type = 'history'
),

enriched as (
    select
        d.weather_date,
        d.city,
        d.weather_code,
        w.weather_description,
        w.weather_category,
        w.severity_score,
        d.actual_temp_max,
        d.actual_temp_min,
        d.actual_temp_mean
    from daily d
    left join {{ ref('dim_weather_code') }} w
      on d.weather_code = w.weather_code
)

select * from enriched
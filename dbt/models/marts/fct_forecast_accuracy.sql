with forecasts as (
    -- historical forecasts: each row is a prediction made on some date for some future date
    select
        cast(ts as date)      as forecast_for_date,
        series                as city,
        forecast              as predicted_temp_max,
        lower_bound           as predicted_lower,
        upper_bound           as predicted_upper,
        cast(dbt_valid_from as date) as forecast_made_on
    from {{ ref('snp_weather_forecast') }}
),

actuals as (
    select
        weather_date          as actual_date,
        city,
        temp_max              as actual_temp_max
    from {{ ref('stg_weather_history') }}
),

joined as (
    select
        f.forecast_made_on,
        f.forecast_for_date,
        f.city,
        f.predicted_temp_max,
        f.predicted_lower,
        f.predicted_upper,
        a.actual_temp_max,
        a.actual_temp_max - f.predicted_temp_max                         as error,
        abs(a.actual_temp_max - f.predicted_temp_max)                    as absolute_error,
        datediff('day', f.forecast_made_on, f.forecast_for_date)         as days_ahead,
        case
            when a.actual_temp_max between f.predicted_lower and f.predicted_upper
            then 1 else 0
        end                                                              as actual_in_interval
    from forecasts f
    inner join actuals a
      on f.forecast_for_date = a.actual_date
     and f.city = a.city
)

select * from joined
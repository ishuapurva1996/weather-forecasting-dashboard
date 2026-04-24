{% snapshot snp_weather_forecast %}

{{
    config(
      target_schema='analytics',
      unique_key="ts || '-' || series",
      strategy='check',
      check_cols=['forecast', 'lower_bound', 'upper_bound'],
      invalidate_hard_deletes=True,
    )
}}

select
    *
from {{ source('forecast', 'weather_forecast_lab1') }}

{% endsnapshot %}
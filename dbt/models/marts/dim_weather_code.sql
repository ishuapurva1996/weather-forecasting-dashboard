select
    code        as weather_code,
    description as weather_description,
    category    as weather_category,
    severity    as severity_score
from {{ ref('wmo_weather_codes') }}
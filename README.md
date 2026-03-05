# HW5 — Weather ETL and Lab1 Forecasting Extension

This repository contains two related pipelines for DATA 226:
- HW5: An Airflow ETL that extracts 60 days of historical weather for a city and loads it into Snowflake.
- Lab1 : A forecasting pipeline that trains a Snowflake ML forecast model on multi-city data and produces 7-day temperature forecasts.

## Project overview
- HW5: scheduled Airflow DAG that calls the Open-Meteo API, transforms daily metrics, and full-refresh loads to Snowflake.
- Lab1: multi-city ETL DAG + forecasting DAG that:
  - creates a training view,
  - trains a Snowflake ML Forecast model,
  - generates 7-day forecasts,
  - unions historical and forecasted data into a unified table.


## Features
- Automated daily extraction of 60 past days of weather data (full-refresh load).
- Transform JSON responses into normalized records (date, temp_max, temp_min, weather_code, lat/lon, city).
- Snowflake persistence with table creation and primary key management.
- Snowflake ML Forecast training and 7-day predictions.
- Unified dataset combining historical and forecasted temperatures for downstream analytics.

## Technologies used
- Apache Airflow (DAGs, tasks, Airflow Variables)
- Python (requests, DAG/task decorators)
- Snowflake (Snowflake SQL, Snowflake ML Forecast)
- Open-Meteo API

## Repository components

- HW5 ETL (weather_api_hw5.py)
  - DAG id: WeatherData_ETL_HW5
  - Schedule: 10 6 * * * (as defined in the DAG)
  - Purpose: single-city ETL
  - Key functions:
    - extract_past_60_days_weather(latitude, longitude) — call Open-Meteo API
    - transform_past_60_days_weather(raw, lat, lon, city) — build records list
    - load(records, target_table) — create table, delete existing rows, insert records
  - Snowflake target: RAW.Weather_ETL_HW5
  - Airflow Variables expected: LATITUDE, LONGITUDE
  - Snowflake connection id: snowflake_conn

- Lab1 (Multi-city ETL + Forecasting )
  - Multi-city ETL (weather_ETL_model.py)
    - DAG id: WeatherData_multiple_cities_data
    - Schedule: 20 22 * * *   (daily at 22:20)
    - Extract + transform tasks executed per city, combined, and loaded   into RAW.Weather_ETL_multiple_cities
    - Airflow Variables expected: city1_LATITUDE, city1_LONGITUDE, city2_LATITUDE, city2_LONGITUDE

  - Forecasting  (forecast_model_temp.py)
    - DAG id: forecast_model_temp_max
    - Schedule: 20 23 * * *   (daily at 23:20, runs after Multi-city ETL)
    - Steps:
      1. Create or replace a training view (RAW.weather_city_view) selecting ds (date), temp_max, city.
      2. Train Snowflake ML Forecast model (ANALYTICS.weather_temperature_lab1) using city as series column.
      3. Produce 7-day forecasts and store in ANALYTICS.weather_forecast_lab1.
      4. Union historical temps from RAW.weather_ETL_multiple_cities with forecast output into ANALYTICS.union_forecast_history_lab1.
    - Airflow Variables / Connections: uses Snowflake via snowflake_conn.

## Deployment & configuration notes
- Ensure Airflow Variables for city coordinates are configured.
- Configure an Airflow connection named `snowflake_conn` with credentials to create tables and run Snowflake ML.
- Review DAG schedules in each file before enabling in production.

## Example use cases
- Temperature trend analysis and visualization.
- Forecast-driven alerts or downstream analytics.
- Demonstration of Airflow + Snowflake + Snowflake ML integration.

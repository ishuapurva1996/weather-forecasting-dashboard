-- ============================================================
-- Weather Forecasting Pipeline — Snowflake Setup
-- Run this as ACCOUNTADMIN or another role with database/schema privileges.
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- Database
CREATE DATABASE IF NOT EXISTS WEATHER_FORECASTING;
USE DATABASE WEATHER_FORECASTING;

-- Schemas
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- ============================================================
-- Optional read-only role for Preset dashboard access
-- Replace <YOUR_SNOWFLAKE_USER> before running the GRANT ROLE statement.
-- ============================================================

CREATE ROLE IF NOT EXISTS DASHBOARD_RO;

GRANT USAGE ON DATABASE WEATHER_FORECASTING TO ROLE DASHBOARD_RO;
GRANT USAGE ON SCHEMA WEATHER_FORECASTING.ANALYTICS TO ROLE DASHBOARD_RO;
GRANT SELECT ON ALL TABLES IN SCHEMA WEATHER_FORECASTING.ANALYTICS TO ROLE DASHBOARD_RO;
GRANT SELECT ON FUTURE TABLES IN SCHEMA WEATHER_FORECASTING.ANALYTICS TO ROLE DASHBOARD_RO;

-- GRANT ROLE DASHBOARD_RO TO USER <YOUR_SNOWFLAKE_USER>;

-- ============================================================
-- Verify setup
-- ============================================================

SHOW SCHEMAS IN DATABASE WEATHER_FORECASTING;
SHOW WAREHOUSES LIKE 'COMPUTE_WH';

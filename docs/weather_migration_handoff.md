# Weather Pipeline Migration Handoff

Date: 2026-06-15

## Current Goal

Move the weather forecasting pipeline from the old Snowflake account/database to the new Snowflake account, keep Preset connected to live Snowflake marts, and mirror the static GitHub Pages dashboard setup from `singhpriyanshu5/us-ev-charging-stations-dashboard`.

## Completed Outside Git

- New Snowflake connection was configured in Airflow as `snowflake_conn`.
- New Snowflake account values used:
  - Account: `OZQYAVQ-DX12220`
  - User: `ADMIN`
  - Role: `ACCOUNTADMIN`
  - Warehouse: `COMPUTE_WH`
  - Database: `WEATHER_FORECASTING`
  - Schema: `ANALYTICS`
- Snowflake database/schemas were created:
  - `WEATHER_FORECASTING`
  - `RAW`
  - `ANALYTICS`
- Airflow pipeline ran successfully after fixes:
  - `WeatherData_multiple_cities_data`
  - `forecast_model_temp_max`
  - `weather_dbt_pipeline`
- dbt tests passed.
- Preset database connection was updated/created as `Snowflake`.
- Preset datasets now point to:
  - Database: `Snowflake`
  - Schema: `analytics`
- Preset dashboard was refreshed and confirmed to show latest data.

## Dashboard Git Commit

Branch:

```text
codex-dashboard-setup
```

Local commit:

```text
7d2daf1 feat: add static weather dashboard
```

This commit includes:

- `web_dashboard/` static Plotly dashboard source.
- `docs/` GitHub Pages deployment copy.
- `.github/workflows/deploy-dashboard.yml` for Snowflake-to-JSON dashboard deploy.
- `dashboard_preview.png`, generated from the provided dashboard PDF.
- README dashboard/setup documentation.
- `sql/snowflake_setup.sql` bootstrap SQL.
- `dags/weather_dbt_dag.py` dashboard deploy trigger after `dbt_test`.

Push status:

```text
Not pushed.
```

Reason:

```text
GitHub HTTPS authentication failed. GitHub requires a Personal Access Token or another configured credential helper.
```

Push command once GitHub auth is fixed:

```bash
git push -u origin codex-dashboard-setup
```

## Remaining Local Changes Not In The Dashboard Commit

These files are intentionally still unstaged/uncommitted:

```text
dags/weather_ETL_model.py
dbt/models/source.yml
```

Purpose of remaining changes:

- `dags/weather_ETL_model.py`
  - Replaces string-formatted Snowflake inserts with parameterized `executemany`.
  - Converts optional numeric weather fields to floats or SQL `NULL`.
  - Skips incomplete daily weather records before loading raw data.
  - Fixes the prior Snowflake numeric conversion failure and dbt null-test failures.
- `dbt/models/source.yml`
  - Replaces hard-coded `USER_DB_BADGER` with `{{ env_var('DBT_DATABASE') }}`.
  - Makes dbt sources portable to the new Snowflake database.

Recommended next commit after dashboard branch is pushed:

```bash
git add dags/weather_ETL_model.py dbt/models/source.yml
git commit -m "fix: harden Snowflake migration pipeline"
```

## GitHub Authentication Fix

The failing push asked for a GitHub username/password. GitHub does not support account passwords for Git pushes.

Use a Personal Access Token as the password:

1. GitHub -> Settings -> Developer settings -> Personal access tokens.
2. Create a token for `ishuapurva1996/weather-forecasting-dashboard`.
3. Grant `Contents: Read and write`.
4. Run:

```bash
git push -u origin codex-dashboard-setup
```

When prompted:

```text
Username: ishuapurva1996
Password: paste the token, not the GitHub password
```

## Verification Already Run

Local checks:

```bash
python3 -m py_compile dags/weather_ETL_model.py dags/weather_dbt_dag.py web_dashboard/export_data.py
git diff --check
git diff --cached --check
```

Runtime checks:

- Airflow ETL load passed.
- Forecast DAG passed.
- dbt DAG passed through `dbt_test`.
- Preset dashboard refreshed with latest data.

## Next Steps

1. Fix GitHub authentication.
2. Push `codex-dashboard-setup`.
3. Open a PR or merge the dashboard setup branch.
4. Commit the remaining migration fixes separately.
5. Configure GitHub Actions secrets if using GitHub Pages deploy:
   - `SNOWFLAKE_ACCOUNT`
   - `SNOWFLAKE_USER`
   - `SNOWFLAKE_PASSWORD`
   - `SNOWFLAKE_DATABASE`
   - `SNOWFLAKE_WAREHOUSE`
   - `SNOWFLAKE_ROLE`
   - optional `SNOWFLAKE_SCHEMA`
6. In GitHub Pages settings, use:
   - Source: Deploy from branch
   - Branch: `main`
   - Folder: `/docs`

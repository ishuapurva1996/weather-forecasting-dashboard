"""
Export Snowflake analytics marts to JSON files for the static dashboard.

Usage:
    cd web_dashboard
    python export_data.py

Required env vars:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE

Optional:
    SNOWFLAKE_SCHEMA defaults to ANALYTICS
"""

import json
import os
import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):
        return False


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(__file__).resolve().parent / "data"
load_dotenv(PROJECT_ROOT / ".env")

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def snowflake_identifier(value, label):
    if not value or not IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid Snowflake {label}: {value!r}")
    return value.upper()


def table_name(table):
    database = snowflake_identifier(os.environ["SNOWFLAKE_DATABASE"], "database")
    schema = snowflake_identifier(os.environ.get("SNOWFLAKE_SCHEMA", "ANALYTICS"), "schema")
    return f"{database}.{schema}.{table}"


QUERIES = {
    "daily_weather": lambda: f"""
        SELECT weather_date, city, actual_temp_max, actual_temp_min,
               actual_temp_mean, weather_code, forecast_temp_max,
               forecast_lower_bound, forecast_upper_bound, record_type
        FROM {table_name("FCT_DAILY_WEATHER")}
        ORDER BY city, weather_date, record_type
        LIMIT 135
    """,
    "forecast_accuracy": lambda: f"""
        SELECT forecast_made_on, forecast_for_date, city, predicted_temp_max,
               predicted_lower, predicted_upper, actual_temp_max, error,
               absolute_error, days_ahead, actual_in_interval
        FROM {table_name("FCT_FORECAST_ACCURACY")}
        WHERE days_ahead BETWEEN 1 AND 7
        ORDER BY city, forecast_for_date, days_ahead
        LIMIT 4001
    """,
    "forecast_revisions": lambda: f"""
        SELECT CAST(dbt_valid_from AS DATE) AS forecast_made_on,
               CAST(ts AS DATE) AS forecast_for_date,
               series AS city,
               forecast AS predicted_temp_max,
               lower_bound AS predicted_lower,
               upper_bound AS predicted_upper,
               CAST(dbt_valid_to AS DATE) AS valid_to
        FROM {table_name("SNP_WEATHER_FORECAST")}
        WHERE CAST(ts AS DATE) >= DATEADD(day, -60, CURRENT_DATE())
        ORDER BY city, forecast_for_date, forecast_made_on
        LIMIT 4001
    """,
    "weather_categories": lambda: f"""
        SELECT weather_date, city, weather_code, weather_description,
               weather_category, severity_score, actual_temp_max,
               actual_temp_min, actual_temp_mean
        FROM {table_name("FCT_WEATHER_CATEGORY_DAILY")}
        ORDER BY city, weather_date
        LIMIT 121
    """,
    "rolling_weather": lambda: f"""
        SELECT city, weather_date, temp_max, temp_min, temp_mean,
               temp_mean_7day_avg, temp_max_7day, temp_min_7day,
               days_in_window
        FROM {table_name("FCT_WEATHER_ROLLING")}
        ORDER BY city, weather_date
        LIMIT 121
    """,
}


def get_connection():
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        login_timeout=30, network_timeout=60, socket_timeout=30,
        session_parameters={"STATEMENT_TIMEOUT_IN_SECONDS": 120},
    )


def serialize(value):
    if isinstance(value, Decimal):
        return float(round(value, 4))
    if isinstance(value, float):
        return round(value, 4)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def normalize_city(value):
    if isinstance(value, str):
        return value.strip().strip('"').strip("'")
    return value


def query_to_records(conn, sql):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        columns = [desc[0].lower() for desc in cursor.description]
        records = []
        for row in cursor.fetchall():
            record = {column: serialize(value) for column, value in zip(columns, row)}
            if "city" in record:
                record["city"] = normalize_city(record["city"])
            records.append(record)
        return records
    finally:
        cursor.close()


def latest(rows, date_key):
    dated_rows = [row for row in rows if row.get(date_key)]
    return max(dated_rows, key=lambda row: row[date_key], default=None)


def mean(values):
    clean = [float(value) for value in values if value is not None]
    return round(sum(clean) / len(clean), 2) if clean else None


def build_kpis(daily_weather, forecast_accuracy, generated_at=None):
    cities = sorted({row["city"] for row in daily_weather if row.get("city")})
    by_city = []

    for city in cities:
        city_daily = [row for row in daily_weather if row["city"] == city]
        city_accuracy = [row for row in forecast_accuracy if row["city"] == city]
        latest_forecast = latest(
            [row for row in city_daily if row["record_type"] == "forecast"],
            "weather_date",
        )
        latest_actual = latest(
            [row for row in city_daily if row["record_type"] == "history"],
            "weather_date",
        )
        hit_values = [
            float(row["actual_in_interval"])
            for row in city_accuracy
            if row.get("actual_in_interval") is not None
        ]

        by_city.append(
            {
                "city": city,
                "latest_forecast_date": latest_forecast.get("weather_date") if latest_forecast else None,
                "latest_forecast_temp_max": latest_forecast.get("forecast_temp_max") if latest_forecast else None,
                "latest_actual_date": latest_actual.get("weather_date") if latest_actual else None,
                "latest_actual_temp_max": latest_actual.get("actual_temp_max") if latest_actual else None,
                "mean_absolute_error": mean(row.get("absolute_error") for row in city_accuracy),
                "interval_hit_rate_pct": round(sum(hit_values) / len(hit_values) * 100, 1) if hit_values else None,
            }
        )

    return {
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "by_city": by_city,
    }


def extract_bundle(conn, source_captured_at, warehouse_completed_at):
    from validate_data import encode_bundle
    exported = {name: query_to_records(conn, factory()) for name, factory in QUERIES.items()}
    exported_at = datetime.now(timezone.utc).isoformat()
    exported['kpis'] = build_kpis(exported['daily_weather'], exported['forecast_accuracy'], generated_at=exported_at)
    bundle = {
        'schema_version': 1,
        'metadata': {
            'source': 'Open-Meteo / Snowflake ML / dbt',
            'source_captured_at': source_captured_at,
            'warehouse_completed_at': warehouse_completed_at,
            'exported_at': exported_at,
            'latest_actual_date': min(r['latest_actual_date'] for r in exported['kpis']['by_city']),
            'expected_refresh_hours': 24,
        },
        'datasets': exported,
    }
    encode_bundle(bundle)
    return bundle


def main():
    import argparse
    from validate_data import encode_bundle
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-captured-at', required=True)
    parser.add_argument('--warehouse-completed-at', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    conn = get_connection()
    try:
        bundle = extract_bundle(conn, args.source_captured_at, args.warehouse_completed_at)
        body, checksum = encode_bundle(bundle)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix('.pending')
        temporary.write_bytes(body)
        os.replace(temporary, args.output)
        print('Validated real weather export:', checksum)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

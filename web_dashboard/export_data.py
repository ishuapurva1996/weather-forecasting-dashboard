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

import snowflake.connector
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)
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
    """,
    "forecast_accuracy": lambda: f"""
        SELECT forecast_made_on, forecast_for_date, city, predicted_temp_max,
               predicted_lower, predicted_upper, actual_temp_max, error,
               absolute_error, days_ahead, actual_in_interval
        FROM {table_name("FCT_FORECAST_ACCURACY")}
        ORDER BY city, forecast_for_date, days_ahead
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
        ORDER BY city, forecast_for_date, forecast_made_on
    """,
    "weather_categories": lambda: f"""
        SELECT weather_date, city, weather_code, weather_description,
               weather_category, severity_score, actual_temp_max,
               actual_temp_min, actual_temp_mean
        FROM {table_name("FCT_WEATHER_CATEGORY_DAILY")}
        ORDER BY city, weather_date
    """,
    "rolling_weather": lambda: f"""
        SELECT city, weather_date, temp_max, temp_min, temp_mean,
               temp_mean_7day_avg, temp_max_7day, temp_min_7day,
               days_in_window
        FROM {table_name("FCT_WEATHER_ROLLING")}
        ORDER BY city, weather_date
    """,
}


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
    )


def serialize(value):
    if isinstance(value, Decimal):
        return float(round(value, 4))
    if isinstance(value, float):
        return round(value, 4)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def query_to_records(conn, sql):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        columns = [desc[0].lower() for desc in cursor.description]
        return [
            {column: serialize(value) for column, value in zip(columns, row)}
            for row in cursor.fetchall()
        ]
    finally:
        cursor.close()


def latest(rows, date_key):
    dated_rows = [row for row in rows if row.get(date_key)]
    return max(dated_rows, key=lambda row: row[date_key], default=None)


def mean(values):
    clean = [float(value) for value in values if value is not None]
    return round(sum(clean) / len(clean), 2) if clean else None


def build_kpis(daily_weather, forecast_accuracy):
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
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "by_city": by_city,
    }


def write_json(filename, data):
    path = DATA_DIR / filename
    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)
        file.write("\n")
    size = len(data) if isinstance(data, list) else "object"
    print(f"wrote {path.relative_to(PROJECT_ROOT)} ({size})")


def main():
    print("Connecting to Snowflake...")
    conn = get_connection()
    exported = {}

    try:
        for name, query_factory in QUERIES.items():
            print(f"Exporting {name}...")
            exported[name] = query_to_records(conn, query_factory())
            write_json(f"{name}.json", exported[name])

        kpis = build_kpis(exported["daily_weather"], exported["forecast_accuracy"])
        write_json("kpis.json", kpis)
    finally:
        conn.close()

    print("Dashboard export complete.")


if __name__ == "__main__":
    main()

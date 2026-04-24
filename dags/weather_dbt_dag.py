from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env": {
        "DBT_USER": "{{ conn.snowflake_conn.login }}",
        "DBT_PASSWORD": "{{ conn.snowflake_conn.password }}",
        "DBT_ACCOUNT": "{{ conn.snowflake_conn.extra_dejson.account }}",
        "DBT_SCHEMA": "{{ conn.snowflake_conn.schema }}",
        "DBT_ROLE": "{{ conn.snowflake_conn.extra_dejson.role }}",
        "DBT_DATABASE": "{{ conn.snowflake_conn.extra_dejson.database }}",
        "DBT_WAREHOUSE": "{{ conn.snowflake_conn.extra_dejson.warehouse }}",
        "DBT_TYPE": "snowflake",
    },
}

with DAG(
    dag_id="weather_dbt_pipeline",
    default_args=default_args,
    description="Run dbt models for weather forecasting pipeline",
    schedule=None,
    start_date=datetime(2025, 3, 19),
    tags=['ELT'],
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="/opt/dbt_venv/bin/dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="/opt/dbt_venv/bin/dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="/opt/dbt_venv/bin/dbt snapshot --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
    )

    dbt_run >> dbt_test >> dbt_snapshot

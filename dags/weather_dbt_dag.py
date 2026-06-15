import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DBT_ENV = {
    "DBT_USER": "{{ conn.snowflake_conn.login }}",
    "DBT_PASSWORD": "{{ conn.snowflake_conn.password }}",
    "DBT_ACCOUNT": "{{ conn.snowflake_conn.extra_dejson.account }}",
    "DBT_SCHEMA": "{{ conn.snowflake_conn.schema }}",
    "DBT_ROLE": "{{ conn.snowflake_conn.extra_dejson.role }}",
    "DBT_DATABASE": "{{ conn.snowflake_conn.extra_dejson.database }}",
    "DBT_WAREHOUSE": "{{ conn.snowflake_conn.extra_dejson.warehouse }}",
    "DBT_TYPE": "snowflake",
}


def trigger_dashboard_deploy():
    token = Variable.get("GITHUB_PAT", default_var=os.environ.get("GITHUB_PAT"))
    repository = Variable.get("GITHUB_REPOSITORY", default_var=os.environ.get("GITHUB_REPOSITORY"))
    branch = Variable.get("GITHUB_BRANCH", default_var=os.environ.get("GITHUB_BRANCH", "main"))
    workflow_file = Variable.get("DASHBOARD_WORKFLOW_FILE", default_var="deploy-dashboard.yml")

    if not token or not repository:
        log.warning("GITHUB_PAT or GITHUB_REPOSITORY not set; skipping dashboard deploy trigger")
        return

    url = f"https://api.github.com/repos/{repository}/actions/workflows/{workflow_file}/dispatches"
    payload = f'{{"ref":"{branch}"}}'.encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        method="POST",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            if response.status not in (200, 201, 202, 204):
                raise RuntimeError(f"Unexpected GitHub dispatch status: {response.status}")
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub workflow dispatch failed: {error.code} {detail}") from error

    log.info("GitHub Actions deploy workflow dispatched successfully")


with DAG(
    dag_id="weather_dbt_pipeline",
    default_args=default_args,
    description="Run dbt models for weather forecasting pipeline",
    schedule=None,
    start_date=datetime(2025, 3, 19),
    tags=['ELT'],
    catchup=False,
) as dag:

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="/opt/dbt_venv/bin/dbt seed --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
        env=DBT_ENV,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="/opt/dbt_venv/bin/dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
        env=DBT_ENV,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="/opt/dbt_venv/bin/dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
        env=DBT_ENV,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="/opt/dbt_venv/bin/dbt snapshot --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
        env=DBT_ENV,
    )

    trigger_deploy = PythonOperator(
        task_id="trigger_dashboard_deploy",
        python_callable=trigger_dashboard_deploy,
    )

    dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test >> trigger_deploy

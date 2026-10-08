"""Build weather marts, then export and dispatch only after every dbt command succeeds."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from weather_publication import run_dbt, export_and_publish, dispatch_dashboard

with DAG(
    dag_id='weather_dbt_pipeline',
    default_args={'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)},
    description='Weather dbt build and validated dashboard publication',
    schedule=None, start_date=datetime(2025, 3, 19), catchup=False,
    max_active_runs=1, tags=['ELT'],
) as dag:
    tasks = [PythonOperator(task_id='dbt_' + command, python_callable=run_dbt,
                            op_kwargs={'command': command}, execution_timeout=timedelta(minutes=12))
             for command in ('seed', 'snapshot', 'run', 'test')]
    export = PythonOperator(task_id='export_dashboard_bundle', python_callable=export_and_publish,
                            execution_timeout=timedelta(minutes=10))
    dispatch = PythonOperator(task_id='trigger_dashboard_deploy', python_callable=dispatch_dashboard)
    tasks[0] >> tasks[1] >> tasks[2] >> tasks[3] >> export >> dispatch

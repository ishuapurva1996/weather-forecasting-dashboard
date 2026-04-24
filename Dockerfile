FROM apache/airflow:2.10.1

USER root
RUN python3 -m venv /opt/dbt_venv && \
    /opt/dbt_venv/bin/pip install --no-cache-dir "dbt-snowflake==1.8.3" && \
    chown -R airflow:root /opt/dbt_venv

USER airflow

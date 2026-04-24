from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import timedelta
from datetime import datetime
import snowflake.connector


def return_snowflake_conn():

      # Initialize the SnowflakeHook
      hook = SnowflakeHook(snowflake_conn_id = 'snowflake_conn')
      
      # Execute the query and fetch results
      conn = hook.get_conn()
      return conn.cursor()


@task
def train_model(target_view, model_name, input_table):
        con = return_snowflake_conn()

        sql_view = f"""CREATE OR REPLACE VIEW {target_view} AS (
                        SELECT 
                        date as ds,
                        temp_max,
                        city
                        FROM {input_table}
                    );"""
        con.execute(sql_view)

        create_model = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {model_name} (
                            INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{target_view}'),    
                            SERIES_COLNAME => 'city',
                            TIMESTAMP_COLNAME => 'ds',          
                            TARGET_COLNAME => 'temp_max',       
                            CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
                        );"""
        con.execute(create_model)


@task
def predict(model_name, forecast_prediction_table):
        con = return_snowflake_conn()

        prediction_sql = f""" BEGIN
            CALL {model_name}!FORECAST(
                FORECASTING_PERIODS => 7,
                CONFIG_OBJECT => {{'prediction_interval': 0.95}}
            );
   
            LET x := SQLID;
            CREATE OR REPLACE TABLE {forecast_prediction_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
            END;"""
       
        con.execute(prediction_sql)

        print(f'{model_name} generated prediction --> stored in {forecast_prediction_table}')

with DAG(
    dag_id = 'forecast_model_temp_max',
    start_date = datetime(2026,3,1),
    catchup=False,
    tags=['ETL'],
    schedule=None,
) as dag:


       target_view = "RAW.weather_city_view"
       input_table = "RAW.weather_ETL_multiple_cities"
       model_name = "ANALYTICS.weather_temperature_lab1"
       forecast_prediction_table = "ANALYTICS.weather_forecast_lab1"

       predict_task = predict(model_name, forecast_prediction_table)

       trigger_dbt = TriggerDagRunOperator(
              task_id='trigger_dbt_pipeline',
              trigger_dag_id='weather_dbt_pipeline',
              wait_for_completion=False,
              reset_dag_run=True,
       )

       train_model(target_view, model_name, input_table) >> predict_task >> trigger_dbt

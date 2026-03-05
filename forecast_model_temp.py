from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

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

@task
def history_predict_union(history_table, forecast_table, union_table):
        con = return_snowflake_conn()

        try:
            con.execute("BEGIN")
            con.execute(f"""CREATE TABLE IF NOT EXISTS {union_table} (
                
                ds              DATE NOT NULL,
                actual          FLOAT,
                city            VARCHAR,
                forecast        FLOAT,
                lower_bound     FLOAT,
                upper_bound     FLOAT);""")

            con.execute(f"""DELETE FROM {union_table};""")

            union_sql = f""" INSERT INTO {union_table} 
                    (ds, actual, city, forecast, lower_bound, upper_bound)
                    SELECT 
                        date as ds,
                        temp_max as actual,
                        city as city,
                        NULL as forecast,
                        NULL as lower_bound,
                        NULL as upper_bound
                    FROM {history_table}

                    UNION

                    SELECT 
                        ts as ds,
                        NULL as actual,
                        series as city,
                        forecast,
                        lower_bound,
                        upper_bound
                    FROM {forecast_table};"""
        
            con.execute(union_sql)
            con.execute("COMMIT")
            

        except Exception as e:
            con.execute("ROLLBACK;")
            print(e)
            raise e
                    




with DAG(
    dag_id = 'forecast_model_temp_max',
    start_date = datetime(2026,3,1),
    catchup=False,
    tags=['ETL'],
    schedule = '20 23 * * *'
) as dag:
       
       
       target_view = "RAW.weather_city_view"
       input_table = "RAW.weather_ETL_multiple_cities"
       model_name = "ANALYTICS.weather_temperature_lab1"
       forecast_prediction_table = "ANALYTICS.weather_forecast_lab1"
       union_table = "ANALYTICS.union_forecast_history_lab1"

       train_model(target_view, model_name, input_table) >> predict(model_name, forecast_prediction_table) >> history_predict_union(input_table, forecast_prediction_table, union_table)

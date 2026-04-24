from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

      # Initialize the SnowflakeHook
      hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
      
      # Execute the query and fetch results
      conn = hook.get_conn()
      return conn.cursor()



# extract function for city1
@task
def extract_past_60_days_weather_city(latitude, longitude):

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
              "latitude": latitude,
              "longitude": longitude,
              "past_days": 60,
              "forecast_days": 0,  # only past weather
              "daily": [
                  "temperature_2m_max",
                  "temperature_2m_min",
                  "temperature_2m_mean",
                  "precipitation_sum",
                  "weather_code"
                  ],
              "timezone": "America/Los_Angeles" #TODO: Update TimeZone
      }

    r = requests.get(url, params=params)

    if r.status_code != 200:
      raise RuntimeError(f'API request failed{r.status_code}')

    data = r.json()
    return data



#transform function for city1
@task
def transform_past_60_days_weather_city(extracted_raw_data, latitude, longitude, city):

        if 'daily' not in extracted_raw_data:
            raise ValueError("'daily' key is not in the API response")
            
        data = extracted_raw_data['daily']
        records = []
        for i in range(len(data['time'])):
            records.append( {
            'latitude':latitude,
            'longitude': longitude,
            'date': data['time'][i],
            'temp_max': data['temperature_2m_max'][i],
            'temp_min':data['temperature_2m_min'][i],
            'temp_mean':data['temperature_2m_mean'][i],
            'weather_code':data['weather_code'][i],
            'city': city
            })
        return records





# Combined the result of city1 and city2
@task
def combine_rec_of_2_cities(city1_data, city2_data):
    
    return city1_data + city2_data


# @task
# def print_length(records):
#     print("Length of records:", len(records))
#     return len(records)



#load func that runs for both cities
@task
def load(records, target_table):
  con = return_snowflake_conn()
  try:
      con.execute("BEGIN")
      con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
        latitude        NUMBER(9,6) NOT NULL,
        longitude       NUMBER(9,6) NOT NULL,
        date            DATE NOT NULL,
        temp_max        FLOAT,
        temp_min        FLOAT,
        temp_mean       FLOAT,          
        weather_code    VARCHAR,
        city            VARCHAR,

        PRIMARY KEY(latitude, longitude, date));""")

      con.execute(f"""DELETE FROM {target_table}""")

      for val in records:
          latitude = val['latitude']
          longitude = val['longitude']
          date = val['date']
          temp_max = val['temp_max']
          temp_min = val['temp_min']
          temp_mean = val['temp_mean']
          weather_code = val['weather_code']
          city = val['city']

          sql = f"""
          INSERT INTO {target_table}
          (latitude, longitude, date, temp_max, temp_min, temp_mean, weather_code, city)
          VALUES ('{latitude}', '{longitude}', '{date}', '{temp_max}', '{temp_min}', '{temp_mean}', '{weather_code}', '{city}');"""
          con.execute(sql)
      con.execute("COMMIT")
      print(f'loaded {len(records)} records in the {target_table}')

  except Exception as e:
      con.execute("ROLLBACK;")
      print(e)
      raise e




with DAG(
    dag_id = 'WeatherData_multiple_cities_data',
    start_date = datetime(2026,3,1),
    catchup=False,
    tags=['ETL'],
    schedule = '20 19 * * *'
) as dag:
    
    target_table = "RAW.Weather_ETL_multiple_cities"

    city1 = 'San Jose'
    city1_LATITUDE = Variable.get("city1_LATITUDE")
    city1_LONGITUDE = Variable.get("city1_LONGITUDE")

    city2 = 'Los Angeles'
    city2_LATITUDE = Variable.get("city2_LATITUDE")
    city2_LONGITUDE = Variable.get("city2_LONGITUDE")

    extracted_raw_data_city1 = extract_past_60_days_weather_city(city1_LATITUDE, city1_LONGITUDE)
    rec1 = transform_past_60_days_weather_city(extracted_raw_data_city1, city1_LATITUDE, city1_LONGITUDE, city1)

    extracted_raw_data_city2 = extract_past_60_days_weather_city(city2_LATITUDE, city2_LONGITUDE)
    rec2 = transform_past_60_days_weather_city(extracted_raw_data_city2, city2_LATITUDE, city2_LONGITUDE, city2)

    rec = combine_rec_of_2_cities(rec1, rec2)

    load_task = load(rec, target_table)

    trigger_forecast = TriggerDagRunOperator(
        task_id='trigger_forecast_dag',
        trigger_dag_id='forecast_model_temp_max',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    load_task >> trigger_forecast

    
    



    

    
    
    













from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

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


#extract func
@task
def extract_past_60_days_weather(latitude, longitude):

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
              "latitude": latitude,
              "longitude": longitude,
              "past_days": 60,
              "forecast_days": 0,  # only past weather
              "daily": [
                  "temperature_2m_max",
                  "temperature_2m_min",
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



#transform func
@task
def transform_past_60_days_weather(extracted_raw_data, latitude, longitude, city):

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
          'weather_code':data['weather_code'][i],
          'city': city
          })
      return records


#load func
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
          weather_code = val['weather_code']
          city = val['city']

          sql = f"""
          INSERT INTO {target_table}
          (latitude, longitude, date, temp_max, temp_min, weather_code, city)
          VALUES ('{latitude}', '{longitude}', '{date}', '{temp_max}', '{temp_min}', '{weather_code}', '{city}');"""
          con.execute(sql)
      con.execute("COMMIT")
      print(f'loaded {len(records)} records in the {target_table}')

  except Exception as e:
      con.execute("ROLLBACK;")
      print(e)
      raise e




with DAG(
    dag_id = 'WeatherData_ETL_HW5',
    start_date = datetime(2026,3,1),
    catchup=False,
    tags=['ETL'],
    schedule = '10 6 * * *'
) as dag:
    
    city = 'San Jose'
    LATITUDE = Variable.get("LATITUDE")
    LONGITUDE = Variable.get("LONGITUDE")

    target_table = "RAW.Weather_ETL_HW5"

    extracted_raw_data = extract_past_60_days_weather(LATITUDE, LONGITUDE)
    rec = transform_past_60_days_weather(extracted_raw_data, LATITUDE, LONGITUDE, city)
    load(rec, target_table)













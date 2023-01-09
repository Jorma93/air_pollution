from datetime import datetime, timedelta
from airflow.models import DAG
from sqlalchemy import create_engine
import pandas as pd
import os,  random, json
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

"""
 GLOBAL VARS AND FUNCTIONS
"""

DAG_ID = 'pipeline'
dag_path = os.getcwd()

first_run_y = 'no'
first_run = True if first_run_y.upper() == 'YES' else False

suffix_pool = '_first' if first_run_y.upper() == 'YES' else ''

def branch_func_first_run():

    print(f"first_run is {first_run}")

    if first_run:
        return 'FIRST_RUN'
    else:
        return 'MANUAL_RUN'

# #extract the data from countries
# def filterData():
#     df=pd.read_csv('/opt/airflow/files/countries.csv')
#     file_csv = df["country_code"]
#     print(file_csv)
pg_hook = PostgresHook(postgres_conn_id="postgres_local")
allowed_countries = ["GB", "FR", "NL"]
required_parameters = ["pm25", "pm10", "o3", "no2", "co"]
pm25_aqi_range = [
  {"value": [0, 50], "average": [0, 12.0]},
  {"value": [51, 100], "average": [12.1, 35.4]},
  {"value": [101, 150], "average": [35.5, 55.4]},
  {"value": [151, 200], "average": [55.5, 150.4]},
  {"value": [201, 300], "average": [150.5, 250.4]},
  {"value": [301, 400], "average": [250.5, 350.4]},
  {"value": [401, 500], "average": [350.5, 500.4]},
  {"value": [501, 999], "average": [500.5, 99999.9]},
]

pm10_aqi_range = [
  {"value": [0, 50], "average": [0, 54.0]},
  {"value": [51, 100], "average": [55.0, 154.0]},
  {"value": [101, 150], "average": [155.0, 254.0]},
  {"value": [151, 200], "average": [255.0, 354.0]},
  {"value": [201, 300], "average": [355.0, 424.0]},
  {"value": [301, 400], "average": [425.0, 504.0]},
  {"value": [401, 500], "average": [505.0, 603.0]},
  {"value": [501, 999], "average": [604.0, 99999.9]},
]

list =[]

def data_manipulation():
  dirs = os.listdir(dag_path + '/files')
  for dir in dirs:
    json = pd.read_json(dag_path + '/files/' + dir, lines=True)
    for index, row in json.iterrows():
      if row["parameter"] in required_parameters \
        and row["country"] in allowed_countries:
        
        pm_25 = None; pm_10 = None

        if row["averagingPeriod"]["value"] == 24:
   
          if row["parameter"] == "pm25":
            for row_pm25 in pm25_aqi_range:
              if row_pm25["average"][0] < row["value"] \
              and row_pm25["average"][1] > row["value"]:
                pm_25 = (row_pm25["value"][1] - row_pm25["value"][0]) / (row_pm25["average"][1] - row_pm25["average"][0]) \
                  * (row["value"] - row_pm25["average"][0]) + row_pm25["value"][0]
    
          if row["parameter"] == "pm10":
            for row_pm10 in pm10_aqi_range:
              if row_pm10["average"][0] < row["value"] \
              and row_pm10["average"][1] > row["value"]:
                pm_10 = (row_pm10["value"][1] - row_pm10["value"][0]) / (row_pm10["average"][1] - row_pm10["average"][0]) \
                  * (row["value"] - row_pm10["average"][0]) + row_pm10["value"][0]
        
        list.append({
          "metric": row["parameter"],
          "value": row["value"],
          "utc_date": row["date"]["utc"],
          "local_date": row["date"]["local"],
          "location": row["location"],
          "country": row["country"],
          "city": row["city"],
          "hourly_unit": row["averagingPeriod"]["value"],
          "pm_25_aqi": pm_25,
          "pm_10_aqi": pm_10
        })

    for item in list:
        columns = item.keys()
        values = [item[column] for column in columns]
        pg_hook.run("""
                    INSERT INTO air_pollution \
                    (metric, value, utc_date, local_date, location, country, city, hourly_unit, pm_25_aqi, pm_10_aqi) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", parameters=tuple(values))
 


args = default_args = {
    'owner': 'Jorma',
    'depends_on_past': False,
    'email': 'jorma.vero@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'tags': ['pips']
}

dag = DAG(dag_id=DAG_ID,
          description=DAG_ID,
          start_date= datetime(2022, 6, 16),
          schedule_interval=None,
          catchup=False,
          max_active_runs=1,
          tags=args['tags'],
          default_args=args)

start = DummyOperator(task_id="Start", dag=dag)

branch_op_run = BranchPythonOperator(task_id='branch_op_run',
                                     provide_context=True,
                                     python_callable=branch_func_first_run,
                                     dag=dag)

FIRST_RUN = DummyOperator(task_id="FIRST_RUN",
                          dag=dag)

MANUAL_RUN = DummyOperator(task_id="MANUAL_RUN",
                          dag=dag)

create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="postgres_local",
    sql="""
        drop table if exists air_pollution;
        create table if not exists air_pollution(
                metric varchar,
                value numeric,
                utc_date timestamp,
                local_date timestamp,
                location varchar,
                country varchar,
                city varchar,
                hourly_unit numeric,
                pm_25_aqi numeric,
                pm_10_aqi numeric
             );
    """,
    dag = dag
)

start_logic = DummyOperator(task_id="start_logic", trigger_rule = "none_failed",
                          dag=dag)


data_manipulation = PythonOperator(
    task_id='data_manipulation',
    python_callable=data_manipulation
)

end = DummyOperator(task_id='End', dag=dag)

"""
RELATIONSHIPS
"""
start >> branch_op_run >> [FIRST_RUN, MANUAL_RUN] 
FIRST_RUN >> create_table >> start_logic
MANUAL_RUN >> start_logic >> data_manipulation 
data_manipulation >> end

if __name__ == "__main__":
    dag.cli()
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
#fa9e3cdf8dc24255b17160302242304

def data_extract():
    import requests
    import json
    url = "http://api.weatherapi.com/v1/forecast.json?key=fa9e3cdf8dc24255b17160302242304&q=chennai&days=3&aqi=no&alerts=no"
    # http://api.weatherapi.com/v1/forecast.json?key=fa9e3cdf8dc24255b17160302242304&q=chennai&days=3&aqi=no&alerts=no
    respond = requests.get(url)
    data = respond.json()

    import datetime
    time_now  = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M') 

    with open(f'/home/aravind/apache_airflow/raw_weather_data/weather_{time_now}.json','wt+') as x:
        json.dump(data,x)

script_path = '/home/aravind/Documents/pyspark_sample.py'
csv_conv = '/home/aravind/Documents/csv_conv.py'
trans_script = '/home/aravind/Documents/transform.py'
ui_script = '/home/aravind/Documents/weather_ui.py'

default_args = {
    'owner' : 'aravind',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2),
    'depends_on_past' : False,
}
with DAG(
    dag_id = 'weather_project',
    description = 'python operator demo perform etl',
    start_date=datetime(2023,12,15,14,54),
    schedule_interval='@daily',
    default_args=default_args,
    catchup = False,
    tags = ['weather_project'],
) as dags:
    task1 = PythonOperator(
        task_id = 'extrate_data',
        python_callable=data_extract
    )
    task2 = BashOperator(
        task_id = 'load_to_MYSQL',
        bash_command=f'python {script_path}',
    )
    task3 = BashOperator(
        task_id = 'move_to_archive',
        bash_command = 'mv /home/aravind/apache_airflow/raw_weather_data/*.json /home/aravind/apache_airflow/weather_archive_folder/ ; mv /home/aravind/apache_airflow/current_data/*.parquet /home/aravind/apache_airflow/archive_current_data/ ; mv /home/aravind/apache_airflow/forcast_data/*.parquet /home/aravind/apache_airflow/archive_forcast_data/ ; mv /home/aravind/apache_airflow/location_data/*.parquet /home/aravind/apache_airflow/archive_location_data/'

        #bash_command = 'mv /home/aravind/apache_airflow/raw_weather_data/*.json /home/aravind/apache_airflow/weather_archive_folder/ | mv /home/aravind/apache_airflow/current_data/*.parquet /home/aravind/apache_airflow/archive_current_data/ | mv /home/aravind/apache_airflow/forcast_data/*.parquet /home/aravind/apache_airflow/archive_forcast_data/ '
    )
    task4 = BashOperator(
        task_id = 'NOSQL_database_mongodb',
        bash_command = "cat /home/aravind/apache_airflow/raw_weather_data/*.json | mongoimport --port 27017 --username root --password root --authenticationDatabase admin --db sampledb --collection aravind --upsert" ,
    )
    task5 = BashOperator(
        task_id = 'json_to_dataframe',
        bash_command=f'python {csv_conv}'

    )
    task6 = BashOperator(
        task_id = 'transformation',
        bash_command = f'python {trans_script}'
    )
    '''task7 = BashOperator(
    task_id='hdfs_copy_task',
    bash_command='hdfs dfs -copyFromLocal /home/aravind/apache_airflow/forcast_data/* /weather/forecast | hdfs dfs -copyFromLocal /home/aravind/apache_airflow/location_data/* /weather/location | hdfs dfs -copyFromLocal /home/aravind/apache_airflow/current_data/* /weather/current', 
    )'''
    '''
    task8 = BashOperator(
        task_id='streamlit_dashboard',
        bash_command=f'python {ui_script}',
    )'''
    task9 = BashOperator(
        task_id = 'open_site',
        bash_command='streamlit run /home/aravind/Documents/weather_ui.py'
    )
    task10 = BashOperator(
        task_id = 'reinforcement_model',
        bash_command='streamlit run /home/aravind/airflow_envi/lib/python3.10/site-packages/airflow/example_dags/model.py'
    )
    task1>>[task4]
    task1 >> task5 >>  task2 >> task6 >>task3
    task6 >> task9
    task6 >> task10
    






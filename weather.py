from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests

defaults_args = {
    'owner': 'Vitali',
    'retrires' : 3
}

def fetchingData(ti):
    url = 'http://api.openweathermap.org/data/2.5/weather?q=Neerach&appid=44a74d24c3165cb6c3670f4d2a5f314d&units=metric'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        temp = data['main']['temp']
        print('fetching data was successful')
        ti.xcom_push(key='weatherData', value=temp)
    else:
        print('some errors accured')

def loadData(ti):
    temp = ti.xcom_pull(key='weatherData', task_ids='fetchAPI')
    print(f'this temp was loaded: {temp}')
    hook = PostgresHook(postgres_conn_id ='myConnection')
    conn = hook.get_conn()
    cursor = conn.cursor()
    sql = f'INSERT INTO Weather (id, temp, date) VALUES (1, {temp}, CURRENT_TIMESTAMP)'
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
    print('sql statement executed')
    


with DAG(
    dag_id = 'weahterETLProzess',
    description = 'Getting weather from api and load into db',
    start_date= datetime(2024,11,26),
    catchup=False,
    schedule_interval= '*/1 * * * *',
    default_args= defaults_args
) as dag:

    fetchingData = PythonOperator(
        task_id = 'fetchAPI',
        python_callable= fetchingData
    )

    loadingData = PythonOperator(
        task_id = 'loadingData',
        python_callable= loadData
    )


fetchingData >> loadingData
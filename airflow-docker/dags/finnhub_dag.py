'''
Description:
This dag pulls periodically stock prices from FinnHub API
and stores it in a Postgres database.

Postgres handling from airflow:
Write:      PostgresOperator
Read:       PostgresHook

Info, structure:
The SQL commands are stored in folder sql to make the code more readable.
'''

import datetime
import logging
import requests
import json

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator # Write to Postgres
from airflow.hooks.postgres_hook import PostgresHook # Read from Postgres


def start():
    logging.info('Starting the DAG YahooFinance API')

def get_data_api():
    url = 'https://finnhub.io/api/v1/quote?'
    querystring = {'symbol': 'CS',
                   'token': 'c66gr5qad3icr57jgts0'}
    try:
        response = requests.get(url, params=querystring, timeout=5)
        response.raise_for_status()
        # Code here will only run if the request is successful
        print('The request was a success!')
        return response.json()
    except requests.exceptions.HTTPError as errh:
        print('HTTP Error occured')
    except requests.exceptions.ConnectionError as errc:
        print('Connection Error occured')
    except requests.exceptions.Timeout as errt:
        print('Timeout Error occured')
    except requests.exceptions.RequestException as err:
        print('Request Exception Error occured')

default_args = {
    'depends_on_past': True
}

dag = DAG(
    dag_id='finnhub_dag',
    schedule_interval='0 */2 * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['finnhub'],
)


greet_task = PythonOperator(
   task_id='start_task',
   python_callable=start,
   dag=dag
)

create_table = PostgresOperator(
    task_id='create_table',
    dag=dag,
    postgres_conn_id='datalake_eldorado',
    sql='sql/create_table_finnhub.sql'
)

insert_values = PostgresOperator(
    task_id='insert_values',
    postgres_conn_id='datalake_eldorado',
    dag=dag,
    sql='sql/insert_values_finnhub.sql',
    params=get_data_api()
)

greet_task >> create_table >> insert_values

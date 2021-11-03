'''
Description:
This dag pulls periodically stock prices from YahooFinance API
and stores it in a defined Postgres database.

Postgres handling from airflow:
Write:      PostgresOperator
Read:       PostgresHook

Info, structure:
The SQL commands are stored in folder sql to make the code more readable.

Resources:
https://marclamberti.com/blog/postgres-operator-airflow/
https://www.nylas.com/blog/use-python-requests-module-rest-apis/
https://www.yahoofinanceapi.com/tutorial
'''

import datetime
import logging
import requests
import json

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator # Write to Postgres
from airflow.hooks.postgres_hook import PostgresHook # Read from Postgres


def start():
    logging.info('Starting the DAG YahooFinance API')

def get_data_api():
    url = "https://yfapi.net/v6/finance/quote?"
    querystring = {'symbols': 'CS',  # if many: 'symbols':'AAPL,BTC-USD,EURUSD=X'
                   'region': 'US',
                   'lang': 'en-US'}
    headers = {'accept': 'application/json',
               'x-api-key': 'TZ1z5Ndk7010iPfALDXa15jAIuLnzLVD2DSFik9e'}
    try:
        logging.info('Start reading API')
        response = requests.get(url, headers=headers, params=querystring, timeout=5)
        response.raise_for_status()
        # Code here will only run if the request is successful
        logging.info('The request was a success!')
        r = response.json()['quoteResponse']['result'][0]
        return r
    except requests.exceptions.HTTPError as errh:
        logging.error('HTTP Error occured')
    except requests.exceptions.ConnectionError as errc:
        logging.error('Connection Error occured')
    except requests.exceptions.Timeout as errt:
        logging.error('Timeout Error occured')
    except requests.exceptions.RequestException as err:
        logging.error('Request Exception Error occured')


dag = DAG(
    'yahoofinance',
    schedule_interval='@hourly',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1)
)

greet_task = PythonOperator(
   task_id="start_task",
   python_callable=start,
   dag=dag
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="datalake_eldorado",
    sql='sql/create_table_yahoofin.sql'
)

insert_values = PostgresOperator(
    task_id="insert_values",
    postgres_conn_id="datalake_eldorado",
    dag=dag,
    sql='sql/insert_values_yahoofin.sql',
    params=get_data_api()
)

greet_task >> create_table >> insert_values

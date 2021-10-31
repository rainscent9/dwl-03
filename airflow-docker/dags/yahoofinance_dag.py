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

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator # Write to Postgres
from airflow.hooks.postgres_hook import PostgresHook # Read from Postgres

response_list=[]
response_dict=dict()

def start():
    logging.info('Starting the DAG YahooFinance AIP')

def get_data_api():
    url = "https://yfapi.net/v6/finance/quote?"

    querystring = {'symbols': 'CS',  # if many: 'symbols':'AAPL,BTC-USD,EURUSD=X'
                   'region': 'US',
                   'lang': 'en-US'}

    headers = {
        'accept': 'application/json',
        'x-api-key': 'TZ1z5Ndk7010iPfALDXa15jAIuLnzLVD2DSFik9e'
    }

    try:
        response = requests.get(url, headers=headers, params=querystring, timeout=5)
        response.raise_for_status()
        # Code here will only run if the request is successful
        print('The request was a success!')
        print(response.json())
    except requests.exceptions.HTTPError as errh:
        print('HTTP Error occured')
    except requests.exceptions.ConnectionError as errc:
        print('Connection Error occured')
    except requests.exceptions.Timeout as errt:
        print('Timeout Error occured')
    except requests.exceptions.RequestException as err:
        print('Request Exception Error occured')

    response = requests.request("GET", url, headers=headers, params=querystring)
    r = response.json()['quoteResponse']['result']
    response_dict = r[0]
    response_list = list(response_dict.values())

def insert_values():
    logging.info('Write values into data base')
    request='sql/insert_values_yahoofin.sql'
    parameters = response_dict
    pg_hook = PostgresHook(postgres_conn_id="datalake_eldorado", schema="datalake1")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    # cursor.execute(request, parameters)
    cursor.execute(request)

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

get_data_api = PythonOperator(
    task_id='get_data_api',
    python_callable=get_data_api,
    dag=dag
)

insert_values = PostgresOperator(
    task_id="insert_values",
    postgres_conn_id="datalake_eldorado",
    dag=dag,
    sql='sql/insert_values_yahoofin.sql',
    parameters=response_dict
)


greet_task >> create_table >> get_data_api >> insert_values

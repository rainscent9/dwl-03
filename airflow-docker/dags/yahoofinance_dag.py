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
    logging.info('Start reading API')
    response = requests.request("GET", url, headers=headers, params=querystring)
    r = response.json()['quoteResponse']['result']
    response_dict = dict()
    # response_dict = {'language': 'en-US', 'region': 'US', 'quoteType': 'EQUITY', 'quoteSourceName': 'NasdaqRealTimePrice', 'triggerable': True, 'currency': 'USD', 'fiftyTwoWeekLowChange': 1.7150002, 'fiftyTwoWeekLowChangePercent': 0.18460712, 'fiftyTwoWeekRange': '9.29 - 14.95', 'fiftyTwoWeekHighChange': -3.9449997, 'fiftyTwoWeekHighChangePercent': -0.26387957, 'fiftyTwoWeekLow': 9.29, 'fiftyTwoWeekHigh': 14.95, 'dividendDate': 1624233600, 'trailingAnnualDividendRate': 0.293, 'trailingPE': 126.494255, 'trailingAnnualDividendYield': 0.02841901, 'epsTrailingTwelveMonths': 0.087, 'epsForward': 1.57, 'sharesOutstanding': 2411269888, 'bookValue': 18.073, 'fiftyDayAverage': 10.191428, 'fiftyDayAverageChange': 0.81357193, 'fiftyDayAverageChangePercent': 0.07982904, 'twoHundredDayAverage': 10.380575, 'twoHundredDayAverageChange': 0.62442493, 'twoHundredDayAverageChangePercent': 0.060153212, 'marketCap': 26904033280, 'forwardPE': 7.009554, 'priceToBook': 0.6089194, 'sourceInterval': 15, 'exchangeDataDelayedBy': 0, 'averageAnalystRating': '3.5 - Hold', 'firstTradeDateMilliseconds': 800631000000, 'shortName': 'Credit Suisse Group', 'priceHint': 2, 'regularMarketChange': 0.6949997, 'regularMarketChangePercent': 6.7410245, 'regularMarketTime': 1635796471, 'regularMarketPrice': 11.005, 'regularMarketDayHigh': 11.04, 'regularMarketDayRange': '10.86 - 11.04', 'regularMarketDayLow': 10.86, 'regularMarketVolume': 8853985, 'regularMarketPreviousClose': 10.31, 'bid': 11.01, 'ask': 11.02, 'bidSize': 40, 'askSize': 32, 'fullExchangeName': 'NYSE', 'financialCurrency': 'CHF', 'regularMarketOpen': 10.88, 'averageDailyVolume3Month': 3905600, 'averageDailyVolume10Day': 2665983, 'exchange': 'NYQ', 'longName': 'Credit Suisse Group AG', 'messageBoardId': 'finmb_405739', 'exchangeTimezoneName': 'America/New_York', 'exchangeTimezoneShortName': 'EDT', 'gmtOffSetMilliseconds': -14400000, 'market': 'us_market', 'esgPopulated': False, 'marketState': 'REGULAR', 'tradeable': False, 'symbol': 'CS'}
    print(response_dict)
    return response_dict

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

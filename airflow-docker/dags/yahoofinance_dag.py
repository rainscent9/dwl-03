import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


def start():
    logging.info('Starting the DAG')

def get_records():
    request = "SELECT * FROM test"
    pg_hook = PostgresHook(postgres_conn_id="datalake_eldorado", schema="datalake1")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    for source in sources:
        logging.info(source)
    return sources

def get_pandas():
    db_hook = PostgresHook(postgres_conn_id="datalake_eldorado", schema="datalake1")
    df = db_hook.get_pandas_df('SELECT * FROM test')
    logging.info(f'Successfully used PostgresHook to return {len(df)} records')
    logging.info(df)


dag = DAG(
        'yahoofinance',
        schedule_interval='@hourly',
        start_date=datetime.datetime.now() - datetime.timedelta(days=1))


greet_task = PythonOperator(
   task_id="start_task",
   python_callable=start,
   dag=dag
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="datalake_eldorado",
    sql='''
        CREATE TABLE IF NOT EXISTS yahoofin (
            id int,
            language text,
            region text,
            quoteType text,
            quoteSourceName text,
            triggerable bool,
            currency text,
            postMarketChangePercent int,
            postMarketTime int,
            postMarketPrice int,
            postMarketChange int,
            regularMarketChange int,
            regularMarketChangePercent int,
            regularMarketTime int,
            regularMarketPrice int,
            regularMarketDayHigh int,
            regularMarketDayRange text,
            regularMarketDayLow int,
            regularMarketVolume int,
            regularMarketPreviousClose int,
            bid int,
            ask int,
            bidSize int,
            askSize int,
            fullExchangeName text,
            financialCurrency text,
            regularMarketOpen int,
            averageDailyVolume3Month int,
            averageDailyVolume10Day int,
            fiftyTwoWeekLowChange int,
            fiftyTwoWeekLowChangePercent int,
            fiftyTwoWeekRange text,
            fiftyTwoWeekHighChange int,
            fiftyTwoWeekHighChangePercent int,
            fiftyTwoWeekLow int,
            fiftyTwoWeekHigh int,
            dividendDate int,
            trailingAnnualDividendRate int,
            trailingPE int,
            trailingAnnualDividendYield int,
            epsTrailingTwelveMonths int,
            marketState text,
            exchange text,
            longName text,
            messageBoardId text,
            exchangeTimezoneName text,
            exchangeTimezoneShortName text,
            gmtOffSetMilliseconds int,
            market text,
            esgPopulated bool,
            epsForward int,
            sharesOutstanding int,
            bookValue int,
            fiftyDayAverage int,
            fiftyDayAverageChange int,
            fiftyDayAverageChangePercent int,
            twoHundredDayAverage int,
            twoHundredDayAverageChange int,
            twoHundredDayAverageChangePercent int,
            marketCap int,
            forwardPE int,
            priceToBook int,
            sourceInterval int,
            exchangeDataDelayedBy int,
            averageAnalystRating text,
            tradeable bool,
            firstTradeDateMilliseconds int,
            priceHint int,
            shortName text,
            symbol text
        )
    '''
)

insert_values = PostgresOperator(
    task_id="insert_values",
    dag=dag,
    postgres_conn_id="datalake_eldorado",
    sql='''
            INSERT INTO yahoofin VALUES (
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?
            )
        '''
)

get_records = PythonOperator(
    task_id="get_records",
    python_callable=get_records,
    dag=dag
)


get_pandas = PythonOperator(
    task_id='get_pandas',
    python_callable=get_pandas,
    dag=dag
    )


greet_task >> create_table >> insert_values >> get_records >> get_pandas

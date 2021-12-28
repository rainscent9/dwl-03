
import datetime as dt
import logging
import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# import psycopg2
# import io

## set parameters to use in API
keyword = 'credit suisse'
yesterday = (dt.date.today() - dt.timedelta(1)).isoformat()

## TASKS
## Get data from APi
def load_data_from_api():
    ''' Get data from news api according to set parameters'''
    url = ('https://newsapi.org/v2/everything?'
           'q=' + keyword + '&from=' + yesterday + '&apiKey=' + os.environ['news_apikey'])
    response = requests.get(url)  # use verify=False, if SSL fails
    data = json.loads(response.content)
    logging.info("End NEWS API call")
    normalized = pd.json_normalize(data, record_path='articles', max_level=1)
    df_news = pd.DataFrame(normalized)
    logging.info("Data transformed to tabular form")
    df_news['keyword'] = keyword
    df_news = df_news.rename({'urlToImage': 'url_image',
                              'publishedAt': 'published_at',
                              'source.id': 'source_id',
                              'source.name': 'source_name'}, axis=1)
    ## connection to database
    engine = create_engine(os.environ['db_connection'])
    ## write dataframe to raw
    df_news.to_sql('news_raw', engine, if_exists="append", index = False)
    logging.info("Data written to database")


## DAGS
dag = DAG(
    "news",
    schedule_interval='@daily',
    start_date=dt.datetime.now() - dt.timedelta(days=1))

get_data_from_api_task = PythonOperator(
    task_id="load_data_from_api",
    python_callable=load_data_from_api,
    dag=dag)


# Configure Task Dependencies
# get_data_from_api_task

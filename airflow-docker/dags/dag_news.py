
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
           'q=' + keyword + '&from=' + yesterday + '&apiKey=cafa16bc58674cbc9ad8a4f0b1c1dff5')
    response = requests.get(url)  # use verify=False, if SSL fails
    data = json.loads(response.content)
    logging.info("End NEWS API call")
    normalized = pd.json_normalize(data, record_path='articles', max_level=1)
    df_news = pd.DataFrame(normalized)
    df_news['keyword'] = keyword
    ## connection to database
    engine = create_engine(
        'postgresql://simon:!GM4Ltcd@datalake-1.cjwwzyskcblj.us-east-1.rds.amazonaws.com:5432/datalake1')
    ## write dataframe to raw
    df_news.to_sql('news_raw', engine, if_exists="append")


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
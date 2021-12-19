import datetime
import logging
import pandas as pd
import psycopg2
from psycopg2 import Error
import psycopg2.extras as extras
import re
from langdetect import detect
# import nltk
# nltk.download('vader_lexicon')
# from nltk.sentiment import SentimentIntensityAnalyzer
# from wordcloud import WordCloud, STOPWORDS

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator


## Define Function for sentiment Analysis
def sentiment_score(text):
  analysis = TextBlob(text)
  if analysis.sentiment.polarity > 0:
    return 1
  elif analysis.sentiment.polarity == 0:
    return 0
  else:
    return -1


def rough_cleaning():
    '''Get data from database and roughly clean some reoccuring patterns'''
    ## connection to database
    try:
        # connect to the PostgreSQL server
        print('Start connection')
        conn = psycopg2.connect(user=os.environ['db_user'],
                                password=os.environ['db_password'],
                                host=os.environ['db_host'],
                                port=os.environ['db_port'],
                                database=os.environ['db_database'])
        conn.set_session(autocommit=True)
        logging.info('Connection successful')

    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
        sys.exit(1)

    ## get cursor for DB-Connection
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT version()')
        logging.info(cursor.fetchone())
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)

    ## read data from database
    df_news_clean = pd.read_sql_query("SELECT * FROM news", conn) ## read data into df

    # cleaning
    df_news_clean['content'].replace('<ul>', '', inplace=True, regex=True)
    df_news_clean['content'].replace('<li>', '', inplace=True, regex=True)
    df_news_clean['content'].replace('</ul>', '', inplace=True, regex=True)
    df_news_clean['content'].replace('</li>', '', inplace=True, regex=True)
    df_news_clean['content'].replace(r'\[\+\d*? chars\]$', '', inplace=True, regex=True)
    df_news_clean['content'] = df_news_clean['content'].str.lower()
    df_news_clean['title'] = df_news_clean['title'].str.lower()
    df_news_clean['description'] = df_news_clean['description'].str.lower()

    ## insert amended values into tuple in to update table
    update_content = tuple(df_news_clean[['content', 'url']].apply(tuple, axis=1))


    ## Update table with cleaned data
    query = """UPDATE news AS t 
                      SET content = e.content
                      FROM (VALUES %s) AS e(content, url) 
                      WHERE e.url = t.url;"""

    try:
        extras.execute_values(cursor, query, update_content)
        logging.info('Updated successfull')
        conn.commit()
        cursor.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)

def language_detection():
    '''Get data from database and detect language of articles'''
    ## connection to database
    try:
        # connect to the PostgreSQL server
        print('Start connection')
        conn = psycopg2.connect(user=os.environ['db_user'],
                                password=os.environ['db_password'],
                                host=os.environ['db_host'],
                                port=os.environ['db_port'],
                                database=os.environ['db_database'])
        conn.set_session(autocommit=True)
        logging.info('Connection successful')

    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
        sys.exit(1)

    ## get cursor for DB-Connection
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT version()')
        logging.info(cursor.fetchone())
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
    conn.set_session(autocommit = True)

        ##  read data into df
        df_news_clean = pd.read_sql_query("SELECT * FROM news", conn)
        ## detect language of title and add as a new colum to df
        df_news_clean['language'] = df_news_clean.apply(lambda row: detect(row.title), axis=1)
        ## Create tuple to insert data
        df_news_clean['tuple'] = df_news_clean[['language', 'url']].apply(tuple, axis=1)
        values_to_insert = tuple(df_news_clean['tuple'])
        ## Update database

        query = """UPDATE news AS t SET language = e.language FROM (VALUES %s) AS e(language, url) WHERE e.url = t.url;"""

        try:
            extras.execute_values(cursor, query, values_to_insert)
            logging.info('Inserted successfull')
            conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            logging.info(error)
        conn.close()

def sentiment_detection():
    '''Get data from database and detect language of articles'''
    ## connection to database
    try:
        # connect to the PostgreSQL server
        print('Start connection')
        conn = psycopg2.connect(user=os.environ['db_user'],
                                password=os.environ['db_password'],
                                host=os.environ['db_host'],
                                port=os.environ['db_port'],
                                database=os.environ['db_database'])
        conn.set_session(autocommit=True)
        logging.info('Connection successful')

    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
        sys.exit(1)

    ## get cursor for DB-Connection
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT version()')
        logging.info(cursor.fetchone())
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)

    ## Read data form database into pandas
    df_news_clean = pd.read_sql_query("SELECT * FROM news WHERE language = 'en';", conn)
    ## Combine all text of news into one column
    df_news_clean['all_text'] = df_news_clean.apply(lambda row:
                                                    row.content + "." + row.title + "." + row.description, axis=1)
    ## detect sentiment of news
    df_news_clean['sentiment_score'] = df_news_clean.apply(lambda row:
                                                           sentiment_score(row.all_text),
                                                           axis=1)
    # create tuple of tuples of values to update
    update_sentiment = tuple(df_news_clean[['sentiment_score', 'url']].apply(tuple, axis=1))





## DAGS

dag = DAG(
    'news.cleaning',
    schedule_interval='@daily',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1)
)

## create table for cleaned news
create_news_table = PostgresOperator(
     task_id="create_table",
     dag=dag,
     postgres_conn_id='datalake_eldorado',
     sql="""CREATE TABLE IF NOT EXISTS news (
        url VARCHAR PRIMARY KEY,
        published_at timestamp,
        author VARCHAR,
        title VARCHAR,
        description VARCHAR,
        content VARCHAR,
        url_image VARCHAR,
        source_id VARCHAR,
        source_name VARCHAR,
        keyword VARCHAR,
        language VARCHAR,
        sentiment_score INT); """)

## insert unique values from news raw
insert_news_values = PostgresOperator(
     task_id="insert_values",
     dag=dag,
     postgres_conn_id='datalake_eldorado',
     sql="""INSERT INTO news (url, published_at, author, title, description, content, url_image, source_id, source_name, keyword)
    SELECT DISTINCT url, published_at::timestamp, author, title, description, content, url_image, source_id, source_name, keyword
    FROM news_raw
    ON CONFLICT (url) DO NOTHING;""")


clean_news_data = PythonOperator(
     task_id='clean_data',
     dag=dag,
     python_callable=rough_cleaning)

detect_language = PythonOperator(
     task_id='add_language',
     dag=dag,
     python_callable=language_detection)

detect_sentiment = PythonOperator(
     task_id='add_sentiment',
     dag=dag,
     python_callable=language_detection)



## Connect tasks
create_news_table >> insert_news_values >> clean_news_data >> detect_language >> detect_sentiment
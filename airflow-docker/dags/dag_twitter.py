import tweepy
import pandas as pd
import numpy as np
import psycopg2

import datetime
import logging
import os

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook



def extract_load_records():
    logging.info('extract & load tweets into Postgres')
    # connecting to twitter api
    consumer_key = os.environ['consumer_key']
    consumer_secret = os.environ['consumer_secret']
    access_token =  os.environ['access_token']
    access_secret = os.environ['access_secret']

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    # Keywords for search
    q = 'CreditSuisse creditsuisse Creditsuisse creditSuisse' + " -filter:retweets AND -filter:replies"

    tweets_list = tweepy.Cursor(api.search_tweets,
                                q=q, tweet_mode='extended',
                                result_type="recent",
                                include_entities=True).items(100000)

    # Connecting to postgres server
    user = os.environ['user']
    password = os.environ['password']
    host = os.environ['host']
    port = os.environ['port']
    database = os.environ['database']
    try:
        conn = psycopg2.connect(user=user,
                                password=password,
                                host=host,
                                port=port,
                                database=database)

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)


    try:
        cur = conn.cursor()
    except psycopg2.Error as er:
        print("Error: Could not get curser to the Database")
        print(er)

    conn.set_session(autocommit=True)

    # Extract tweets

    for tweet in tweets_list:
        text = tweet._json["full_text"]
        created_at = str(tweet.created_at)
        twitter_id = tweet.id_str
        user_id = tweet.user.id_str
        username = tweet.user.name
        user_location = tweet.user.location
        number_followers = tweet.user.followers_count
        retweet_count = tweet.retweet_count
        likes = tweet.favorite_count
        media_source = tweet.source
        tweet_language = tweet.lang
        retweeted = tweet.retweeted

    # Loading to Postgres

        if not hasattr(tweet, "retweeted_status" and "quoted_status"):  #

            query = '''INSERT INTO Twitter (twitter_id, created_at, user_id, username, user_location, retweeted, text, number_followers, retweet_count, likes,
                media_source, tweet_language) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);''' #ON CONFLICT (twitter_id) DO NOTHING

            cur.execute(query, (
            twitter_id, created_at, user_id, username, user_location, retweeted, text, number_followers, retweet_count,
            likes, media_source, tweet_language))
            conn.commit()

    cur.close()
    conn.close()

default_args = {
    'depends_on_past': False
}

dag = DAG(
    dag_id='twitter_tweets',
    schedule_interval='2 * * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['twitter'],
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="datalake_eldorado",
    sql='''CREATE TABLE IF NOT EXISTS Twitter (twitter_id varchar, created_at varchar, user_id varchar, username varchar, user_location varchar, retweeted varchar, text varchar, number_followers int, retweet_count int, likes int, 
    media_source varchar, tweet_language varchar) ;'''
)

extract_load = PythonOperator(
    task_id="extract_load_records",
    python_callable=extract_load_records,
    dag=dag)


create_table >> extract_load

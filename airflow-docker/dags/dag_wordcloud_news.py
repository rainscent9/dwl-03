import datetime
import logging
import pandas as pd
import psycopg2
import nltk
nltk.download('vader_lexicon')
nltk.download('wordnet')
from wordcloud import WordCloud, STOPWORDS

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator


## set connection parameter for DB
user = 'simon'
password = '!GM4Ltcd'
host = 'datalake-1.cjwwzyskcblj.us-east-1.rds.amazonaws.com'
port = '5432'
database = "datalake1"




def create_wordcloud():
    '''Get data from database and create word cloud '''

    ## CONNECTION
    ## connection to database
    try:
        # connect to the PostgreSQL server
        print('Start connection')
        conn = psycopg2.connect(user=user,
                                password=password,
                                host=host,
                                port=port,
                                database=database)
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


    ## GET DATA
    ## set timeframe (last 30 days)
    prev_30_day = (dt.date.today() - dt.timedelta(30)).isoformat()
    today = dt.date.today().isoformat()

    # get data from DB for within timeframe
    query = """SELECT title, content, description, published_at::Date
    FROM news WHERE language = 'en' and '[" + prev_30_day + ", " + today + "]'::daterange @> published_at::DATE;"""
    df_news_clean = pd.read_sql_query(query, conn)

    ## DATA QUALITY
    # Concatinate all text per row
    df_news_clean['all_text'] = df_news_clean.apply(lambda row:
                                                    row.content + "." + row.title + "." + row.description,
                                                    axis=1)
    # create one big text from all the articles
    text = ". ".join(df_news_clean['all_text'])
    # make all lowercase & use casefold to clean data
    text = text.casefold()
    # split text into list to lemmanitze data
    text_list = text.split(" ")
    text_list = [Word(word).lemmatize() for word in text_list]
    print(text_list)
    # concat text again
    text = ' '.join(text_list)

    ## WORDCLOUD
    # Create stopword list & add custom stop words
    stopwords = set(STOPWORDS)
    custom_stopwords = ['credit', 'suisse', 'reuters', 'bloomberg',
                        'motley', 'fool', 'wire', 'content',
                        'contents', 'target', '_blank', 'image', 'source',
                        'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    for word in custom_stopwords:
        stopwords.add(word)

    # create wordcloud
    wordcloud = WordCloud(stopwords=stopwords,  # use custom stopwords
                          collocations=True,  # allow often subsequent words
                          max_words=20).generate(text) ## top 20 words
    words_dict = wordcloud.words_

    ## PREP DATA TO INSERT INTO DB
    # Convert into df & set parameters to pass to DB
    data = [[today, period, words_string, 'news']]
    df = pd.DataFrame(list(words_dict.items()), columns=['word', 'frequency'])
    df[['channel', 'to_date', 'from_date']] = 'news', today, prev_30_day

    # Inserting each word
    for i in df.index:

        query = """ INSERT into wordcloud(word, frequency, channel, to_date, from_date)
        values('%s', '%s', '%s', '%s', '%s');""" % (
            df['word'].values[i],
            df['frequency'].values[i],
            df['channel'].values[i],
            df['to_date'].values[i],
            df['from_date'].values[i])

        try:
            cursor.execute(query)
            print('Add of Column successfull')
            conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


## DAGS

dag = DAG(
    'news.cleaning',
    schedule_interval='@daily',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1)
)

## create table for word cloud data
create_news_table = PostgresOperator(
     task_id="create_table",
     dag=dag,
     postgres_conn_id='datalake_eldorado',
     sql="""CREATE TABLE IF NOT EXISTS wordcloud (word VARCHAR, frequency NUMERIC, channel VARCHAR, to_date date, from_date date);""")


word_cloud = PythonOperator(
     task_id='word_cloud',
     dag=dag,
     python_callable=create_wordcloud)




## Connect tasks
create_news_table >> word_cloud
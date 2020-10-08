import datetime as dt
import os
import logging
import psycopg2
import re

from google.cloud import storage
import pandas as pd
from pandahouse import to_clickhouse, read_clickhouse
from dotenv import load_dotenv
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG


args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 10, 2),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,

}

load_dotenv()


connection = {'host': 'http://localhost:8123',
              'database': 'new_york',
              'user': None,
              'password': None}

DATA_DIR = os.getenv('DATA_DIR')


TABLES_DICT = {'number_trips': '''CREATE TABLE IF NOT EXISTS new_york.number_trips (date Date, count UInt64) 
                                  ENGINE=ReplacingMergeTree(date, (date), 8192)''',
               'average_duration': '''CREATE TABLE IF NOT EXISTS new_york.average_duration (date Date, 
                              avg_duration Float64) ENGINE=ReplacingMergeTree(date, (date), 8192)''',
               'gender_number_trips': '''CREATE TABLE IF NOT EXISTS new_york.gender_number_trips (date Date,
                                 gender UInt8, count UInt64) ENGINE=ReplacingMergeTree(date, (date), 8192)'''}

client_google_storage = storage.Client()
bucket_bicycle = client_google_storage.get_bucket('bucket_bicycle')
bucket_for_statistics = client_google_storage.get_bucket('bucket_for_statistics')


logging.basicConfig(filename='../app.log', level=logging.INFO, format='%(asctime)s %(message)s')


def create_buckets():
    client_google_storage.create_bucket('bucket_bicycle')
    client_google_storage.create_bucket('bucket_for_statistics')


def remove_files():
    '''
    Удаление файлов
    :return:
    '''
    list_files = os.listdir(DATA_DIR)
    for file in list_files:
        os.remove(DATA_DIR + file)


def send_statistics():
    pattern = r'([0-9]+)'
    files = os.listdir(DATA_DIR)
    for file in files:
        date_str = re.search(pattern, file).group()
        date = dt.datetime.strptime(date_str, '%Y%m')

        try:
            date_next_month = date.replace(month=date.month+1)
        except ValueError:
            if date.month == 12:
                date_next_month = date.replace(year=date.year+1, month=1)
            else:
                raise
        query = '''
            SELECT * FROM new_york.{table} WHERE (date >= toDate('{date}')) and (date < toDate('{date_next_month}'))
        '''

        for table in TABLES_DICT:
            query = query.format(table=table, date=date, date_next_month=date_next_month)
            df = read_clickhouse(query, index='date', connection=connection)
            filename = f'Statistics-{file}'
            blob = bucket_for_statistics.blob(filename)
            blob.upload_from_string(df.to_string(index=False, justify='left'), content_type='text/csv')


def set_status_is_download():
    files = os.listdir(DATA_DIR)
    for file in files:
        pass


def check_and_download_files():
    '''
    Получение списка всех файлов
    :return:
    '''
    for blob in bucket_bicycle.list_blobs():  # Цикл по проверке новых файлов
        filename = blob.name
        flag_file = False
        with psycopg2.connect(dbname='bservice', user='khanze') as client_psql:
            cursor = client_psql.cursor()
            # Пометить файл в базу данных
            try:
                cursor.execute('''INSERT INTO file_bucket (name) VALUES ('{filename}');'''.format(filename=filename))
                logging.info('{filename} file is written to the database'.format(filename=filename))
                flag_file = True
            except psycopg2.Error as e:
                error = e.pgerror
                logging.error('PSQL: {}'.format(error))

        if flag_file is False:
            continue

        with open('data/{}'.format(filename), mode='wb') as csv_file:
            csv_file.write(blob.download_as_string())


def pivot_dataset_count_numbers():
    '''
    Загрузка кол-ва поездок в день
    Подсчет идет от даты начала
    :return:
    '''
    data_files_list = os.listdir(DATA_DIR)

    for file in data_files_list:
        try:
            df = pd.read_csv(DATA_DIR + file, compression='zip')
        except:
            continue

        df['date'] = pd.to_datetime(df['starttime'])  # создаем новую колонку date
        df['date'] = df['date'].dt.date  # форматируем колонку в тип данных date
        df = df.value_counts('date').to_frame(name='count')  # подсчет строк по колонке date и форматируем в DataFrame

        to_clickhouse(df, table='number_trips', connection=connection)  # загрузка в clickhouse


def pivot_dataset_average_trip_duration():
    '''
    Загрузка средней продолжительности поездок в день
    :return:
    '''
    data_files_list = os.listdir(DATA_DIR)

    for file in data_files_list:
        try:
            df = pd.read_csv(DATA_DIR + file, compression='zip')
        except:
            continue

        df['date'] = pd.to_datetime(df['starttime'])
        df['date'] = df['date'].dt.date
        df['starttime'] = pd.to_datetime(df['starttime'])
        df['stoptime'] = pd.to_datetime(df['stoptime'])
        df['trip seconds'] = df['stoptime'] - df['starttime']
        df['trip seconds'] = df['trip seconds'].dt.total_seconds()
        df_count_sum_trips = df.groupby('date')['trip seconds'].agg(count_trips='count', sum_trip_seconds='sum')
        df_count_sum_trips['avg_duration'] = df_count_sum_trips['sum_trip_seconds'] / df_count_sum_trips['count_trips']
        df_date_avg = df_count_sum_trips[['avg_duration']]

        to_clickhouse(df_date_avg, table='average_duration', connection=connection)


def pivot_dataset_trip_gender():
    '''
    Распределение поездок пользователей по категории "gender"
    :return:
    '''

    data_files_list = os.listdir(DATA_DIR)

    for file in data_files_list:
        try:
            df = pd.read_csv(DATA_DIR + file, compression='zip')
        except:
            continue

        df['date'] = pd.to_datetime(df['starttime'])
        df['date'] = df['date'].dt.date
        df_gender = df.groupby(['gender', 'date'])['tripduration'].agg(count='count')

        to_clickhouse(df_gender, table='gender_number_trips', connection=connection)


with DAG(dag_id='new_york_dataset_pivot', default_args=args, schedule_interval=None) as dag:
    check_files = PythonOperator(
        task_id='check_and_download_files',
        python_callable=check_and_download_files,
        dag=dag
    )

    pivot_number_trips = PythonOperator(
        task_id='pivot_dataset_count_numbers',
        python_callable=pivot_dataset_count_numbers,
        dag=dag
    )

    pivot_average_duration = PythonOperator(
        task_id='pivot_dataset_average_trip_duration',
        python_callable=pivot_dataset_average_trip_duration,
        dag=dag
    )

    pivot_gender_trip = PythonOperator(
        task_id='pivot_dataset_trip_gender',
        python_callable=pivot_dataset_trip_gender,
        dag=dag
    )

    send_statistics_task = PythonOperator(
        task_id='send_statistics',
        python_callable=send_statistics,
        dag=dag

    )

    check_files >> [pivot_gender_trip, pivot_average_duration, pivot_number_trips] >> send_statistics_task

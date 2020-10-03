import logging
import psycopg2
import os
import datetime as dt

from google.cloud import storage
from dotenv import load_dotenv
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from import_data import pivot_dataset_average_trip_duration, pivot_dataset_count_numbers, pivot_dataset_trip_gender

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 10, 2),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,

}

load_dotenv()

client_google_storage = storage.Client()
bucket = client_google_storage.get_bucket('bucket_bicycle')
DATA_DIR = os.getenv('DATA_DIR')

logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s %(message)s')


def remove_files():
    '''
    Удаление файлов
    "Если пригодится"
    :return:
    '''
    list_files = os.listdir(DATA_DIR)
    for file in list_files:
        os.remove(DATA_DIR + file)


def check_and_download_files():
    '''
    Получение списка всех файлов
    :return:
    '''
    for blob in bucket.list_blobs():  # Цикл по проверке новых файлов
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


with DAG(dag_id='new_york_dataset_pivot', default_args=args, schedule_interval=None) as dag:
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

# 2. обернуть в airflow
# 3. message in telegram
# 4. docker-compose


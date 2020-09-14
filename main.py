import os
import logging
import psycopg2

from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

client_google_storage = storage.Client()
bucket = client_google_storage.get_bucket('bucket_bicycle')

logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s %(message)s')


def check_file(filename):
    """
    Проверка файла в Базе, new or old
    :type filename: str
    False - файл был использован в загрузке данных
    True - файл не был использован в загрузке данных
    """
    with psycopg2.connect(dbname='bservice', user='khanze') as client_psql:
        cursor = client_psql.cursor()
        # Пометить файл в базу данных
        try:
            cursor.execute('''INSERT INTO file_bucket (name) VALUES ('{filename}');'''.format(filename=filename))
            logging.info('{filename} file is written to the database'.format(filename=filename))
            flag = True
        except psycopg2.Error as e:
            error = e.pgerror
            flag = False
            logging.error('PSQL: {}'.format(error))
    return flag


def download_blob():
    '''
    Получение списка всех файлов
    Загрузка в папку файлов и извлечение из zip
    :return:
    '''
    for blob in bucket.list_blobs():  # Цикл по проверке новых файлов
        filename = blob.name
        flag_file = check_file(blob.name)
        if flag_file is False:
            continue

        with open('data/{}'.format(filename), mode='wb') as csv_file:
            csv_file.write(blob.download_as_string())
    # После загрузки данных файла, нужно добавить удаление из локальной директории, чтобы не засорять память


# 1. удаление после использования
# 2. обернуть в airflow
# 3. message in telegram
# 4. docker-compose


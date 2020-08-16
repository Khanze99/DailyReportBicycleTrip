import os
import logging
import uuid
import datetime

from google.cloud import storage
from clickhouse_driver import Client


client_google_storage = storage.Client()
client_clickhouse = Client('localhost')
bucket = client_google_storage.get_bucket('bucket_bicycle')
encoding = 'utf-8'

# 1. Перед тем как начать прогать новое, пройдись и порефакторь код
# 2. Логирование настроить


def check_file_db(file_name):
    # Проверка файлов в базе данных

    files = client_clickhouse.execute('SELECT * FROM bservice.file_bucket')
    for file in files:
        if file_name == file[2]:
            return True
    return False


def insert_file(file_name):
    # Пометить файл в базу данных
    client_clickhouse.execute('''INSERT INTO bservice.file_bucket (id, date, name) 
                                 VALUES ('{id_file}', '{date}', '{file_name}')'''.format(id_file=uuid.uuid4(),
                                                                                         date=datetime.datetime.now().date(),
                                                                                         file_name=file_name))


for blob in bucket.list_blobs():  # Цикл по проверке новых файлов
    file_name = blob.name
    flag_file_db = check_file_db(file_name=file_name)
    if flag_file_db is True:
        continue

    with open('{}'.format(blob.name), mode='wb') as csv_file:
        csv_file.write(blob.download_as_string())
        if 'zip' in blob.name:
            os.system('unzip {file}'.format(file=file_name))
        insert_file(blob.name)



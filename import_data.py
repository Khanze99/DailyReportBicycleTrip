import datetime
import os


import pandas as pd
from pandahouse import to_clickhouse
from dotenv import load_dotenv

load_dotenv()


connection = {'host': 'http://localhost:8123',
              'database': 'new_york',
              'user': None,
              'password': None}

DATA_DIR = os.getenv('DATA_DIR')

# pandahouse еще до конца не разобрался с пакетом, запрашиваю execute и пишу команды sql вручную,
# думаю в скором времени нужно будет разобрать глубже


CREATE_TABLES_DICT = {'number_trips': '''CREATE TABLE IF NOT EXISTS new_york.number_trips (date Date, count UInt64) 
                                  ENGINE=MergeTree(date, (date), 8192)''',
                      'average_duration': '''CREATE TABLE IF NOT EXISTS new_york.average_duration (date Date, 
                                      avg_duration Float64) ENGINE=MergeTree(date, (date), 8192)''',
                      'gender_number_trips': '''CREATE TABLE IF NOT EXISTS new_york.gender_number_trips (date Date,
                                         gender UInt8, count UInt64) ENGINE=MergeTree(date, (date), 8192)'''}


# 0 = not known,
# 1 = male,
# 2 = female,


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

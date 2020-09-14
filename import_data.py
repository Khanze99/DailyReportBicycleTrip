import datetime
import os


import pandas as pd
from pandahouse import to_clickhouse, read_clickhouse
from pandahouse.http import execute
from dotenv import load_dotenv

load_dotenv()


connection = {'host': 'http://localhost:8123',
              'database': 'new_york',
              'user': None,
              'password': None}

DATA_DIR = os.getenv('DATA_DIR')

# pandahouse еще до конца не разобрался с пакетом, запрашиваю execute и пишу команды sql вручную,
# думаю в скором времени нужно будет разобрать глубже


def create_tables():
    '''
    Создание всех таблиц для записи аналитики
    :return:
    '''
    create_dict = {'number_trips': '''CREATE TABLE IF NOT EXISTS new_york.number_trips (date Date, count UInt64) 
                                      ENGINE=MergeTree(date, (date), 8192)''',
                   'average_duration': '''CREATE TABLE IF NOT EXISTS new_york.average_duration (date Date, 
                                          avg_duration Float64) ENGINE=MergeTree(date, (date), 8192)''',
                   'gender_number_trips': '''CREATE TABLE IF NOT EXISTS new_york.gender_number_trips (date Date,
                                             gender UInt8, count UInt64) ENGINE=MergeTree(date, (date), 8192)'''}
    execute(create_dict['number_trips'], connection=connection)
    execute(create_dict['average_duration'], connection=connection)
    execute(create_dict['gender_number_trips'], connection=connection)


def insert_average_duration(df):
    '''
    Запись средней продолжительности поездки в день
    :param df:
    :return:
    '''

    dict_seconds_trips = {}  # {date: {seconds: int}}
    query = '''INSERT INTO new_york.average_duration(date, avg_duration) VALUES('{date}',
               {avg_duration})'''

    for row in df[['starttime', 'stoptime']].iterrows():
        start_datetime = datetime.datetime.strptime(row[1].array[0], '%Y-%m-%d %H:%M:%S')
        stop_datetime = datetime.datetime.strptime(row[1].array[1], '%Y-%m-%d %H:%M:%S')
        diff = start_datetime - stop_datetime
        diff_seconds = diff.seconds
        start_datetime_str = start_datetime.date().strftime('%Y-%m-%d')
        stop_datetime_str = stop_datetime.date().strftime('%Y-%m-%d')

        if start_datetime.date() == stop_datetime.date():  # Тут суммирую время поездок, которые произошли в один день

            if start_datetime_str in dict_seconds_trips:
                dict_seconds_trips[start_datetime_str]['seconds'] += diff_seconds
            else:
                dict_seconds_trips[start_datetime_str] = {'seconds': diff_seconds}

        elif start_datetime.date() != stop_datetime.date():  # Кейс с 2 днями, так же есть шанс такого что поездка
                                                            # будет больше 2 дней, пока не придумал решения лучше,
                                                            #  но думаю оно возможно
            if start_datetime_str in dict_seconds_trips:
                dict_seconds_trips[start_datetime_str]['seconds'] += diff_seconds
            else:
                dict_seconds_trips[start_datetime_str] = {'seconds': diff_seconds}

            if stop_datetime_str in dict_seconds_trips:
                dict_seconds_trips[stop_datetime_str]['seconds'] += diff_seconds
            else:
                dict_seconds_trips[stop_datetime_str] = {'seconds': diff_seconds}

    for trip in dict_seconds_trips:
        count = read_clickhouse('''SELECT count from new_york.number_trips
                                           where date == '{start_datetime_str}' '''.format(
            start_datetime_str=trip), connection=connection)['count'].array[0]
        avg = dict_seconds_trips[trip]['seconds'] / count
        execute(query.format(date=trip, avg_duration=avg), connection=connection)


def insert_gender_number_trips(df):
    '''
    Запись кол-во поездок по гендерному признаку(так я понял условие)
    :param df:
    :return:
    '''

    dict_gender_number_trips = {}  # {data: {gender: count int}}
    query = '''INSERT INTO new_york.gender_number_trips(date, gender, count) VALUES('{date}', {gender}, {count})'''

    for row in df[['starttime', 'stoptime', 'gender']].iterrows():
        array = row[1].array
        start_datetime = datetime.datetime.strptime(array[0], '%Y-%m-%d %H:%M:%S')
        stop_datetime = datetime.datetime.strptime(array[1], '%Y-%m-%d %H:%M:%S')
        gender = array[2]
        start_datetime_str = start_datetime.date().strftime('%Y-%m-%d')
        stop_datetime_str = stop_datetime.date().strftime('%Y-%m-%d')

        if start_datetime_str != stop_datetime_str:
            if start_datetime_str in dict_gender_number_trips:
                if gender in dict_gender_number_trips[start_datetime_str]:
                    dict_gender_number_trips[start_datetime_str][gender] += 1
                else:
                    dict_gender_number_trips[start_datetime_str][gender] = 1
            else:
                dict_gender_number_trips[start_datetime_str] = {gender: 1}

            if stop_datetime_str in dict_gender_number_trips:
                if gender in dict_gender_number_trips[stop_datetime_str]:
                    dict_gender_number_trips[stop_datetime_str][gender] += 1
                else:
                    dict_gender_number_trips[stop_datetime_str][gender] = 1
            else:
                dict_gender_number_trips[stop_datetime_str] = {gender: 1}

        elif start_datetime_str == stop_datetime_str:
            if start_datetime_str in dict_gender_number_trips:
                if gender in dict_gender_number_trips[stop_datetime_str]:
                    dict_gender_number_trips[start_datetime_str][gender] += 1
                else:
                    dict_gender_number_trips[stop_datetime_str][gender] = 1
            else:
                dict_gender_number_trips[start_datetime_str] = {gender: 1}

    for trip in dict_gender_number_trips:
        for gender in dict_gender_number_trips[trip]:
            execute(query.format(date=trip, gender=gender,
                                 count=dict_gender_number_trips[trip][gender]), connection=connection)  # в pandahouse
            # используется to_clickhouse с использование DataFrame, изучить, возможно ускорится процесс загрузки

    # 0 = not known,
    # 1 = male,
    # 2 = female,


def pivot_dataset_count_numbers():
    '''
    Загрузка кол-ва поездок в день
    Подсчет идет от даты начала
    :return:
    '''
    data_files_list = os.listdir(DATA_DIR)  # получение списка файлов в директории
    for file in data_files_list:  # прогоняем каждый файл и загружаем, ловим exception в случае если файл не тот
        try:
            city_bike_trip_df = pd.read_csv(DATA_DIR + file, compression='zip')
        except:
            continue

        city_bike_trip_df['date'] = pd.to_datetime(city_bike_trip_df['starttime'])  # создаем новую колонку date
        city_bike_trip_df['date'] = city_bike_trip_df['date'].dt.date  # форматируем колонку в тип данных date
        count_trips_df = city_bike_trip_df.value_counts('date').to_frame(name='count')  # подсчет строк по колонке date
        to_clickhouse(count_trips_df, table='number_trips', connection=connection)  # загрузка в clickhouse


pivot_dataset_count_numbers()

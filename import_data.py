import pandas as pd
from pandahouse import to_clickhouse
from pandahouse.http import execute
import datetime

connection = {'host': 'http://localhost:8123',
              'database': 'new_york',
              'user': None,
              'password': None}


def create_tables():
    create_dict = {'number_trips': '''CREATE TABLE IF NOT EXISTS new_york.number_trips (date Date, count UInt64) 
                                      ENGINE=MergeTree(date, (date), 8192)''',
                   'average_duration': '''CREATE TABLE IF NOT EXISTS new_york.average_duration (date Date, avg_duration Float32)
                                          ENGINE=MergeTree(date, (date), 8192)''',
                   '': ''''''}
    execute(create_dict['number_trips'], connection=connection)
    execute(create_dict['average_duration'], connection=connection)


def insert_number_trips(df):
    dict_count_trips = {}
    query = '''INSERT INTO new_york.number_trips(date, count) VALUES('{date}', {count})'''
    for row in df[['starttime', 'stoptime']].iterrows():
        start_time_date_str = row[1].array[0].split(' ')[0]
        stop_time_date_str = row[1].array[1].split(' ')[0]
        if start_time_date_str != stop_time_date_str:
            if start_time_date_str in dict_count_trips:
                dict_count_trips[start_time_date_str] += 1
            else:
                dict_count_trips[start_time_date_str] = 1
            if stop_time_date_str in dict_count_trips:
                dict_count_trips[stop_time_date_str] += 1
            else:
                dict_count_trips[stop_time_date_str] = 1
        else:
            if start_time_date_str in dict_count_trips:
                dict_count_trips[start_time_date_str] += 1
            else:
                dict_count_trips[start_time_date_str] = 1
    for trip in dict_count_trips:
        execute(query.format(date=datetime.datetime.strptime(trip, '%Y-%m-%d').date(),
                             count=dict_count_trips[trip]), connection=connection)


def insert_average_duration(df):
    pass


def start(filename):
    dir_file = 'data/{filename}'.format(filename=filename)
    df = pd.read_csv(dir_file, delimiter=',')
    create_tables()
    insert_number_trips(df)
    insert_average_duration(df)


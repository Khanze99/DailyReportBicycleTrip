import pandas as pd
from pandahouse import to_clickhouse

connection = {'host': 'localhost',
              'database': 'bservice'}


def create_database_clickhouse(name):
    pass


read_csv = pd.read_csv('data/201306-citibike-tripdata.csv',
                       encoding='utf-8')


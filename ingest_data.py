#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pandas as pd

import pyarrow.parquet as pq
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = 'output.parquet'

    os.system(f"wget {url} -O {parquet_name}")

    # download the parquet file

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    trips = pq.read_table(parquet_name)
    trips = trips.to_pandas()

    trips['tpep_pickup_datetime'] = pd.to_datetime(trips['tpep_pickup_datetime'])
    trips['tpep_dropoff_datetime'] = pd.to_datetime(trips['tpep_dropoff_datetime'])


    #engine.connect()

    #print(pd.io.sql.get_schema(trips, name ="yellow_taxi_data",con=engine))

    trips.to_sql(name=table_name,con=engine,if_exists='replace')

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres.')


    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='passward for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port',type=int, help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name where we write the results to')
    parser.add_argument('--url', help='url for the parquet file')


    args = parser.parse_args()

    main(args)


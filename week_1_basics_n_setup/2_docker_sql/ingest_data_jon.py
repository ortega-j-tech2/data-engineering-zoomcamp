#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from sqlalchemy import create_engine
import argparse

from time import time


#####################################################################################################################
def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = 'taxi_data_parquet.parquet'
    csv_name = 'output.csv'

    os.system(f"curl -o {parquet_name} {url}")
    
    #####################
    # Added this block since now files are in PARQUET format
    # Also, this format comes with a first column that I need to remove
    df = pd.read_parquet(parquet_name)
    print('Converting file from parquet to csv...', flush=True)
    df.to_csv(csv_name)
    print('Converting file finished!', flush=True)

    print('reading csv file to drop first column...', flush=True)
    df = pd.read_csv(csv_name)
    print('reading csv file...done!', flush=True)
    first_column = df.columns[0]

    print('dropping column from df...', flush=True)
    df = df.drop([first_column], axis=1)
    print('dropping column from df...done!', flush=True)

    print('saving file...', flush=True)
    df.to_csv(csv_name, index=False)
    print('saving file...done!',flush=True)
    #####################

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True,chunksize=10000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')



    while True:
        t_start= time()
        
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end= time()
        print('inserted another chunk...took %.3f seconds' % (t_end - t_start), flush=True)

#####################################################################################################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                        prog='ingest_data.py',
                        description='Ingest CSV data to Postrgres',
                        epilog='')

    parser.add_argument('--user',help='user name for postgres')           # positional argument
    parser.add_argument('--password',help='passord for postgres')           # positional argument
    parser.add_argument('--host',help='host for postgres')           # positional argument
    parser.add_argument('--port',help='port for postgres')           # positional argument
    parser.add_argument('--db',help='database name for postgres')           # positional argument
    parser.add_argument('--table_name',help='name of the table we will write the results to')           # positional argument
    parser.add_argument('--url',help='url of the csv file')           # positional argument

    args= parser.parse_args()

    main(args)
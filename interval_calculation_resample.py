import pandas as pd
import psycopg2 as pg
import argparse

def freq_transform(args):
    table = args.freq_transform[0]
    #Get table from postgre
    engine = pg.connect(database='my_db', user='postgres', password='Kelly1030', host='127.0.0.1', port='5432')
    df = pd.read_sql('select * from ' + table, con=engine)

    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df.set_index('open_time', inplace=True)
    #Use resample to roll up time frequency from 1 min to 15 min
    df = df.resample('15min').agg({
        'open': lambda s: s[0],
        'high': lambda df: df.max(),
        'low': lambda df: df.min(),
        'close': lambda df: df[-1],
        'volume': lambda df: df.sum()
    })
    print(df)

def main():
    # create parser object
    parser = argparse.ArgumentParser()
    # defining arguments for parser object
    parser.add_argument("-fT", "--freq_transform", type=str, nargs=1,
                        metavar=("table"), default=None,
                        help="Transform data frequency")
    args = parser.parse_args()
    freq_transform(args)

if __name__ == '__main__':
    main()








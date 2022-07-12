import pandas as pd
import psycopg2 as pg
import argparse

def freq_transform(args):
    table = args.freq_transform[0]
    freq = args.freq_transform[1]

    #Get table from postgre
    engine = pg.connect(database='my_db', user='postgres', password='Kelly1030', host='127.0.0.1', port='5432')
    df = pd.read_sql('select * from ' + table, con=engine)

    #Empty arrays and dataframe to store results
    df2 = pd.DataFrame()
    symbol = []
    vendor = []
    open_time = []
    open = []
    high = []
    low = []
    close = []
    close_time = []
    volume = []
    n_trades = []

    #frq = 15m, 1h, 1d
    if freq == "15m":
        for i in range(0, len(df), 15):
            if (i+14)<=len(df):
                symbol.append(df.iloc[i]['symbol_id'])
                vendor.append(df.iloc[i]['vendor_id'])
                open.append(df.iloc[i]['open'])
                open_time.append(df.iloc[i]['open_time'])
                high_value = max(df['high'][i:(i+15)])
                high.append(high_value)
                low_value = min(df['low'][i:(i+15)])
                low.append(low_value)
                close_time.append(df.iloc[i+14]['close_time'])
                close.append(df.iloc[i + 14]['close'])
                volume_value = sum(df['volume'][i:(i+15)])
                volume.append(volume_value)
                trades_value = sum(df['n_trades'][i:(i + 15)])
                n_trades.append(trades_value)
            else:
                break

        df2["symbol_id"] = symbol
        df2["vendor_id"] = vendor
        df2["open_time"] = open_time
        df2["open"] = open
        df2["high"] = high
        df2["low"] = low
        df2["close"] = close
        df2["close_time"] = close_time
        df2["n_trades"] = n_trades
        df2["volume"] = volume

    elif freq == "1h":
        for i in range(0, len(df), 60):
            if (i + 59) <= len(df):
                symbol.append(df.iloc[i]['symbol_id'])
                vendor.append(df.iloc[i]['vendor_id'])
                open.append(df.iloc[i]['open'])
                open_time.append(df.iloc[i]['open_time'])
                high_value = max(df['high'][i:(i + 60)])
                high.append(high_value)
                low_value = min(df['low'][i:(i + 60)])
                low.append(low_value)
                close_time.append(df.iloc[i + 59]['close_time'])
                close.append(df.iloc[i + 59]['close'])
                volume_value = sum(df['volume'][i:(i + 60)])
                volume.append(volume_value)
                trades_value = sum(df['n_trades'][i:(i + 60)])
                n_trades.append(trades_value)
            else:
                break

        df2["symbol_id"] = symbol
        df2["vendor_id"] = vendor
        df2["open_time"] = open_time
        df2["open"] = open
        df2["high"] = high
        df2["low"] = low
        df2["close"] = close
        df2["close_time"] = close_time
        df2["n_trades"] = n_trades
        df2["volume"] = volume

    print(df2)

def main():
    # create parser object
    parser = argparse.ArgumentParser()
    # defining arguments for parser object
    parser.add_argument("-fT", "--freq_transform", type=str, nargs=2,
                        metavar=("table", "freq"), default=None,
                        help="Transform data frequency")
    args = parser.parse_args()
    freq_transform(args)

if __name__ == '__main__':
    main()







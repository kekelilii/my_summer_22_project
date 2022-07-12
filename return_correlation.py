import pandas as pd
import psycopg2 as pg
import argparse
import psycopg2.extras as extras

#Calculate currency return
def calReturn ():
    #Connect to table
    engine = pg.connect(database='my_db', user='postgres', password='Kelly1030', host='127.0.0.1', port='5432')
    df = pd.read_sql('select * from kline_new', con=engine)
    #Set datetime as index
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df.set_index('open_time', inplace=True)
    #Seperate currencies using groupby
    currency = df.groupby('symbol_id')
    #We will calculate daily returns and monthly returns (on 1 day frequency data)
    daily_returns = []
    monthly_returns = []
    for group in currency:
        df0 = pd.DataFrame(group[1])
        daily = df0['close'].pct_change()
        monthly = df0['close'].resample('M').ffill().pct_change()
        daily_returns.append(daily)
        monthly_returns.append(monthly)

    daily_returns = pd.DataFrame(daily_returns).T
    daily_returns.columns = ['BTCUSDT', 'ETHUSDT', 'DOGEUSDT', 'LTCUSDT']
    monthly_returns = pd.DataFrame(monthly_returns).T
    monthly_returns.columns = ['BTCUSDT', 'ETHUSDT', 'DOGEUSDT', 'LTCUSDT']

    return daily_returns

#Calculate the correlation between returns
def correlation(args):
    mtd = args.correlation[0]
    cor = calReturn().corr(method=mtd)
    print(cor)

#Table to store return results
def createTable(args):
    conn = pg.connect(
        database="my_db", user='postgres', password='Kelly1030', host='127.0.0.1', port='5432'
    )
    conn.autocommit = True
    cursor = conn.cursor()
    table = args.createTable[0]
    sql = (
            """ 
        CREATE TABLE """ + table + """ (
                open_time timestamp,
                BTCUSDT double precision,
                ETHUSDT double precision,
                DOGEUSDT double precision,
                LTCUSDT double precision
                );
            """)
    cursor.execute(sql)
    print("Tables created successfully........")
    cursor.close()

def exportData(args, df):
    #Export to postgre
    conn = pg.connect(
        database= 'my_db', user= 'postgres', password= 'Kelly1030', host= '127.0.0.1', port= '5432'
    )
    conn.autocommit = True
    cursor = conn.cursor()
    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join([str(i) for i in list(df.columns)])
    # SQL query to execute
    table = args.exportData[0]
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, pg.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()

def main():
    # create parser object
    parser = argparse.ArgumentParser()
    # defining arguments for parser object
    parser.add_argument("-eD", "--exportData", type=str, nargs=1,
                        metavar="table", default=None,
                        help="Export return data into its table.")
    parser.add_argument("-cT", "--createTable", type=str, nargs=1,
                        metavar="table", default=None,
                        help="Create tables in database.")
    parser.add_argument("-C", "--correlation", type=str, nargs=1,
                        metavar="mtd", default=None,
                        help="Calculate the correlation between currency returns.")
    args = parser.parse_args()

    if args.exportData != None:
        df = calReturn().reset_index()
        exportData(args, df)
    elif args.createTable != None:
        createTable(args)
    elif args.correlation != None:
        correlation(args)

if __name__ == '__main__':
    main()

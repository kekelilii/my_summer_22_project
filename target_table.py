import pandas as pd
import psycopg2 as pg
import argparse
import psycopg2.extras as extras

#Create table to store target data
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
                run_id VARCHAR(55) NOT NULL,
                symbol_id SERIAL NOT NULL,
                vendor_id SERIAL NOT NULL,
                open_time BIGINT NOT NULL,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                close_time BIGINT NOT NULL,
                n_trades INTEGER,
                CONSTRAINT """ + table + """_PK PRIMARY KEY (run_id, symbol_id, vendor_id, 
                open_time, close_time)
                );
        """
    )
    cursor.execute(sql)
    print("Tables created successfully........")
    cursor.close()

#Drop columns and transform to target data types
def transformData():
    engine = pg.connect(database='my_db', user='postgres', password='Kelly1030', host='127.0.0.1', port='5432')
    df = pd.read_sql('select * from kline_new_raw', con=engine)
    #drop columns
    df = df.drop(columns=['ignore', 'quote_asset_volume', 'taker_buy_base_asset', 'taker_buy_quote_asset'])
    #change data types
    df = df.astype({"open": float, "high": float, "low": float, "close": float, "volume": float})
    return df

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
    parser.add_argument("-cT", "--createTable", type=str, nargs=1,
                        metavar="table", default=None,
                        help="Create tables in database.")
    parser.add_argument("-eD", "--exportData", type=str, nargs=1,
                        metavar="table", default=None,
                        help="Export transformed data into its table.")
    args = parser.parse_args()

    if args.createTable != None:
        createTable(args)
    elif args.exportData != None:
        df = transformData()
        exportData(args, df)

if __name__ == '__main__':
    main()

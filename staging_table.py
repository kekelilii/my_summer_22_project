import psycopg2
import pandas as pd
import argparse
import uuid as uuid
import psycopg2.extras as extras

#Connect to Postgre and create a database
def createDB(args):
    # establishing the connection
    conn = psycopg2.connect(
        database="postgres",
        user='postgres',
        password='Kelly1030',
        host='127.0.0.1',
        port='5432')
    conn.autocommit = True
    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    name = args.createDB[0]
    sql = 'CREATE database ' + name
    # Creating a database
    cursor.execute(sql)
    print("Database created successfully........")
    # Closing the connection
    conn.close()

#Create tables to store staging data
def createTable(args):
    conn = psycopg2.connect(
        database="my_db",
        user='postgres',
        password='Kelly1030',
        host='127.0.0.1',
        port='5432')
    conn.autocommit = True
    cursor = conn.cursor()
    table1 = args.createTable[0]
    id1 = args.createTable[1]
    name1 = args.createTable[2]
    table2 = args.createTable[3]
    id2 = args.createTable[4]
    name2 = args.createTable[5]
    table3 = args.createTable[6]
    sql = (
        """
        CREATE TABLE """ + table1 + """(
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        );
        """,
        """
        INSERT INTO """ + table1 + """
        VALUES(""" + id1 + """,'""" + name1 + """');
        """,
        """
        CREATE TABLE """ + table2 + """ (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL
                );
        """,
        """
        INSERT INTO """ + table2 + """
        VALUES(""" + id2 + """,'""" + name2 + """');
        """,
        """
        CREATE TABLE """ + table3 + """ (
                run_id VARCHAR(55) NOT NULL,
                symbol_id SERIAL NOT NULL,
                vendor_id SERIAL NOT NULL,
                open_time BIGINT NOT NULL,
                open VARCHAR(55),
                high VARCHAR(55),
                low VARCHAR(55),
                close VARCHAR(55),
                volume VARCHAR(55),
                close_time BIGINT NOT NULL,
                quote_asset_volume VARCHAR(55),
                n_trades INTEGER,
                taker_buy_base_asset VARCHAR(55),
                taker_buy_quote_asset VARCHAR(55),
                ignore VARCHAR(55),
                CONSTRAINT """ + table3 + """_PK PRIMARY KEY (run_id, symbol_id, vendor_id,
                open_time, close_time)
                );
        """
    )
    for command in sql:
        cursor.execute(command)
    print("Tables created successfully........")
    cursor.close()

#Query ID information from tables
def queryData(table, name):
    conn = psycopg2.connect(
        database="my_db",
        user='postgres',
        password='Kelly1030',
        host='127.0.0.1',
        port='5432')
    conn.autocommit = True
    cursor = conn.cursor()
    sql = "SELECT id FROM " + table + " WHERE name = '" + name + "';"
    cursor.execute(sql)
    id = int(cursor.fetchone()[0])
    cursor.close()
    return id

def exportData(args, vendor_id, symbol_id):
    csv_file = args.exportData[0]
    table = args.exportData[1]
    # Read CSV file into DataFrame df
    df = pd.read_csv(
        csv_file,
        header=None,
        dtype={      #Keep the raw data types
            1: str,
            2: str,
            3: str,
            4: str,
            5: str,
            7: str,
            9: str,
            10: str,
            11: str})
    # Add ID columns based on the query results
    run_id = str(uuid.uuid1())
    df.insert(0, 'run_id', run_id)
    df.insert(1, 'symbol_id', symbol_id)
    df.insert(2, 'vendor_id', vendor_id)
    df.columns = [    #Give column names
        'run_id',
        'symbol_id',
        'vendor_id',
        'open_time',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'close_time',
        'quote_asset_volume',
        'n_trades',
        'taker_buy_base_asset',
        'taker_buy_quote_asset',
        'ignore']
    # Export to postgre
    conn = psycopg2.connect(
        database="my_db",
        user='postgres',
        password='Kelly1030',
        host='127.0.0.1',
        port='5432')
    conn.autocommit = True
    cursor = conn.cursor()
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join([str(i) for i in list(df.columns)])
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
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
    parser.add_argument(
        "-cD",
        "--createDB",
        type=str,
        nargs=1,
        metavar="name",
        default=None,
        help="Create a database in PostgreSQL.")
    parser.add_argument(
        "-cT",
        "--createTable",
        type=str,
        nargs=7,
        metavar=(
            "table1",
            "id1",
            "name1",
            "table2",
            "id2",
            "name2",
            "table3"),
        default=None,
        help="Create tables in database.")
    parser.add_argument(
        "-eD",
        "--exportData",
        type=str,
        nargs=2,
        metavar=(
            "csv_file",
            "table"),
        default=None,
        help="Generate a dataframe from csv, add ID columns, and export to table")
    # parse the arguments from standard input
    args = parser.parse_args()

    if args.createDB is not None:
        createDB(args)
    elif args.createTable is not None:
        createTable(args)
    elif args.exportData is not None:
        vendor_id = queryData("vendors", "Binance")
        symbol_id = queryData("symbols", "BTCUSDT")
        exportData(args, vendor_id, symbol_id)


if __name__ == '__main__':
    main()

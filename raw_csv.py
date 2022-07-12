import requests
import csv
import argparse
from datetime import datetime

vendor_source = {
    "binance": "https://api.binance.com/api/v3/klines"
}

def importData(args):
    symbol = args.importData[0]
    interval = args.importData[1]
    vendor = args.importData[2]
    start_time = int(datetime(2021, 1, 1).timestamp()) * 1000
    end_time = int(datetime(2022, 1, 1).timestamp()) * 1000

    response = requests.get('https://api.binance.com/api/v3/klines',
                                params={'symbol': symbol,
                                        'interval': interval,
                                        'startTime': start_time,
                                        'endTime': end_time,
                                        "limit": 100000})

    with open(symbol + '_' + interval + '_' + vendor + '.csv', 'w', newline='') as f:
        wr = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        for line in response.json():
            wr.writerow(line)

file_format = "%s_%s_%s"
class InputFile:
    def __init__(self, symbol, interval, vendor):
        self.symbol = symbol
        self.interval = interval
        self.vendor = vendor

    def getInputFilePath(self):
        '''
        Reader or writer can use this path to find the file
        '''
        return f"{self.symbol}_{self.interval}_{self.vendor}.csv"

def main():
    # create parser object
    parser = argparse.ArgumentParser()
    # defining arguments for parser object
    parser.add_argument(
        "-iD",
        "--importData",
        type=str,
        nargs=3,
        metavar=(
            'symbol',
            'interval',
            'vendor'),
        default=None,
        help="Save data into csv file.")

    # parse the arguments from standard input
    args = parser.parse_args()

    if args.importData != None:
        importData(args)

# url = 'https://api.binance.com/api/v3/klines' BTCUSDT 1m Binance
if __name__ == '__main__':
    main()

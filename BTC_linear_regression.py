import csv
import warnings
from datetime import datetime
import numpy as np
import pandas as pd
import requests
from sklearn.linear_model import LinearRegression
warnings.filterwarnings("ignore")

def importData():
    symbol = 'BTCUSDT'
    interval = '1h'
    start_time = int(datetime(2022, 6, 1).timestamp()) * 1000
    end_time = int(datetime(2022, 6, 16).timestamp()) * 1000

    response = requests.get('https://api.binance.com/api/v3/klines',
                                params={'symbol': symbol,
                                        'interval': interval,
                                        'startTime': start_time,
                                        'endTime': end_time,
                                        "limit": 100000})
    #Write data into csv
    with open('data/linearRegressionBTC.csv', 'w', newline='') as f:
        wr = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        for line in response.json():
            wr.writerow(line)

#CLean data: drop columns, adjust data types...
def clean(csv):
    df = pd.read_csv(csv, header=None)
    df = df.drop(columns=[7,9,10,11])
    df.columns = ['opentime', 'open', 'high', 'low', 'close', 'volume', 'closetime', 'n_trades']
    df = df.astype({"open": float, "high": float, "low": float, "close": float, "volume": float})
    df["datetime"] = pd.to_datetime(df['opentime'], unit='ms')
    return df

#Volume weighted average price as our regression output Y
def vwap(df):
    v = df['volume'].values
    tp = (df['low'] + df['close'] + df['high']).div(3).values
    return df.assign(vwap=(tp * v).cumsum() / v.cumsum())

#Create the regression model and Do hourly transactions based on the model
def regression(df):
    start = 0 #Start time of the 7-day training set
    coin = 0 #Number of coins owned
    money = 0 #Money traded
    while (start < 168):
        end = 7 * 24 + start
        btc7 = df.iloc[start:end, :]
        # Create time dummy as our X
        btc7["time_index"] = np.arange(len(btc7.index))
        # Training data
        X = btc7.loc[:, ['time_index']]
        y = btc7.loc[:, 'vwap']
        # Train the model
        model = LinearRegression()
        model.fit(X, y)
        #Get slope and intercept
        slope = model.coef_
        intercept = model.intercept_
        #Test
        test_time_index = end
        predicted_price = test_time_index * slope + intercept
        real_price = df.loc[test_time_index, 'vwap']
        #Decide buy or sell (real price>=predicted price: Sell; <: Buy)
        if predicted_price <= real_price: buy = False
        else: buy = True
        #Loop
        start += 1

        #Do the transaction
        if buy == False:
            coin -= 1
            money += real_price
        elif buy == True:
            coin += 1
            money -= real_price

    #Clearing our coins and see if profit or loss
    final = money + coin * real_price
    return coin, money, final

def main():
    #importData()
    btc = clean("data/linearRegressionBTC.csv")
    btc = vwap(btc)
    print("Coin, money =", regression(btc))


if __name__ == '__main__':
    main()
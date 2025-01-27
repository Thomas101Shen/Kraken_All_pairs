import time
import krakenex
from pykrakenapi import KrakenAPI
import traceback
import os
from load_env import load_env
import pandas as pd

load_env()
api_key = os.getenv("api_key")
api_secret = os.getenv("api_secret")

api = krakenex.API(api_key, api_secret)
kraken = KrakenAPI(api)
pair = 'BTCUSD'
interval = 1
max_bars = 60  # neccessary bars for calculations

try:
    # Fetch initial data
    ohlc, last = kraken.get_ohlc_data(pair, interval)
    print(ohlc.columns)
except Exception as e:
    print(f"Error during initialization: {e}")
    exit(1)

ohlc = ohlc.head(max_bars)

while True:
    try:
        now = time.time()
        prev_interval = now % (interval * 60)
        sleep_time = interval * 60 - prev_interval
        print(f"seconds till next update: {sleep_time}")
        time.sleep(sleep_time)
        new_ohlc, last = kraken.get_ohlc_data(pair, interval, since=last)
        ohlc = pd.concat([new_ohlc, ohlc]).head(max_bars)
        ohlc.to_csv("./ohlc.csv")

    except Exception as e:
        if "public call frequency exceeded" in str(e):
            print("Rate limit exceeded. Retrying in 1 second...")
            time.sleep(1)
        else:
            error_message = f"An error occurred: {e}\n{traceback.format_exc()}"
            print(error_message)
            with open("error_log.txt", "a") as error_log:
                error_log.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {error_message}\n")

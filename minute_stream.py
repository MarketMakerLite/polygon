from polygon import WebSocketClient, STOCKS_CLUSTER
import pandas as pd
from sqlalchemy import create_engine
import datetime
from datetime import datetime, timezone
import time
import json
import threading
import config
import traceback
import requests

print("starting stream...")
key = config.polygon_key
engine = create_engine(config.psql)


def unix_convert(ts):
    ts = int(ts/1000)
    tdate = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return tdate


def save_data(message):
    try:
        df = pd.DataFrame.from_dict(message)
        df.drop('ev', axis=1, inplace=True)
        if 'op' in df.columns:
            df.columns = ['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open',
                          'tick_close', 'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end']
        else:
            df.insert(3, 'op', None)
            df.columns = ['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open',
                      'tick_close', 'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end']
        df['tdate'] = df['time_end'].map(lambda x: unix_convert(x))
        df['save_date'] = datetime.utcnow()
        print(df)
        df.to_sql('stockdata', engine, if_exists='append', index=False, method='multi', chunksize=100000)
    except Exception:
        traceback.print_exc()
    return None


def my_custom_process_message(message):
    message = json.loads(message)
    if message[0]['ev'] != 'status':
        print(datetime.now(tz=timezone.utc), "streaming: ", message[0]['sym'])
        threading.Timer(1, save_data, [message]).start()
    return None


def my_custom_error_handler(ws, error):
    raise ValueError('an error happened:', error)


def my_custom_close_handler(ws):
    print("closed connection")
    return None


def main():
    my_client = WebSocketClient(STOCKS_CLUSTER, key,
                                my_custom_process_message, my_custom_close_handler, my_custom_error_handler)
    my_client.run_async()
    try:
        my_client.subscribe('AM.*')
        my_client.close_connection()
    except Exception:
        traceback.print_exc()
        my_client.close_connection()
    return None


def internet_check():
    url = "https://socket.polygon.io"
    timeout = 15
    try:
        requests.get(url, timeout=timeout)
        connected = True
    except (requests.ConnectionError, requests.Timeout):
        connected = False
    return connected


if __name__ == "__main__":
    connected = internet_check()
    while connected:
        try:
            main()
            connected = internet_check()
        except Exception:
            traceback.print_exc()
            time.sleep(1)
            connected = internet_check()
            while not connected:
                connected = internet_check()
                time.sleep(5)
            continue
    while not connected:
        connected = internet_check()
        time.sleep(5)
        continue

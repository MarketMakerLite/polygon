import polygon
from polygon import StreamClient, enums
import datetime
from datetime import datetime
import time
import threading
import config
import traceback
import requests
import redis
import json

print("starting stream...")
key = config.polygon_key

def connections():
    redis_pool = redis.ConnectionPool(host=config.redis_host, port=config.redis_port, db=0, password=config.redis_pw)
    # redis_pool = redis.ConnectionPool(connection_class=redis.UnixDomainSocketConnection, path="/var/run/redis/redis-server.sock",
    #                                   password=config.redis_pw, db=0)
    r = redis.Redis(connection_pool=redis_pool, charset="utf-8", decode_responses=True)
    print('redis connected', r)
    return r


def redis_message(messages):
    for message in messages:
        r.rpush('stock-list', json.dumps(message))
    return None


def unix_convert(ts):
    ts = int(ts/1000)
    tdate = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return tdate


def save_data(message):
    z = time.time()
    keys = {'sym': 'symbol', 'v': 'tick_volume', 'av': 'total_volume', 'op': 'opening_price', 'vw': 'tick_vwap',
            'o': 'tick_open', 'c': 'tick_close', 'h': 'tick_high', 'l': 'tick_low', 'a': 'vwap', 'z': 'avg_trade_size',
            's': 'time_beg', 'e': 'time_end'}

    # Drop Unknown Keys
    key_count = len(message[0].keys())
    if key_count > len(keys.keys())+1:
        message = [{k: single[k] for k in keys if k in single} for single in message]
        print('New fields detected! Check API documentation: https://polygon.io/docs/websockets/')
    else: 
        message = [{k: single[k] for k in keys if k in single} for single in message]

    new_message = []
    for d in message:
        # del d['ev']  # delete status
        if 'op' not in d:
            d['op'] = None
        d = {keys[name]: val for name, val in d.items()}
        d['tdate'] = unix_convert(d['time_end'])
        d['save_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        cols = ['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open', 'tick_close',
                'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date']
        d = {k: d[k] for k in cols}
        new_message.append(d)
    redis_message(new_message)
    # print(message)
    print(datetime.utcnow(), 'stock 1s agg', time.time()-z)
    return None


def my_custom_process_message(ws, msg):
    print(msg)
    message = json.loads(msg)
    if message[0]['ev'] != 'status':
        threading.Thread(target=save_data, args=[message]).start()
    return None


def my_custom_error_handler(ws, error):
    raise ValueError('an error happened:', error)


def my_custom_close_handler(ws, close_code, close_msg):
    print("closed connection", close_code, close_msg)
    return None


def main():
    my_client = polygon.StreamClient(key, polygon.enums.StreamCluster('stocks'), host='delayed.polygon.io'
                                     , on_message=my_custom_process_message, on_close=my_custom_close_handler
                                     , on_error=my_custom_error_handler)
    # my_client = polygon.StreamClient(key, polygon.enums.StreamCluster('crypto'), on_message=my_custom_process_message,
    #                                  on_close=my_custom_close_handler, on_error=my_custom_error_handler)
    try:
        my_client.start_stream_thread()
        my_client.subscribe_stock_second_aggregates(symbols=['SPCE'])
        # my_client.subscribe_crypto_minute_aggregates()
        # my_client.subscribe_stock_trades()
    except Exception:
        traceback.print_exc()
        my_client.unsubscribe_stock_second_aggregates()
        # my_client.unsubscribe_crypto_minute_aggregates()
        # my_client.unsubscribe_stock_trades_aggregates()
    return None


def internet_check():
    url = "https://socket.polygon.io"
    timeout = 15
    try:
        requests.get(url, timeout=timeout)
        connected = True
        print('Internet connected')
    except (requests.ConnectionError, requests.Timeout):
        connected = False
        print('No Internet')
    return connected


if __name__ == "__main__":
    connected = internet_check()
    if connected:
        try:
            r = connections()
            main()
        except Exception:
            traceback.print_exc()
            time.sleep(1)
            connected = internet_check()
            while not connected:
                connected = internet_check()
                time.sleep(5)
    while not connected:
        connected = internet_check()
        time.sleep(5)
        continue


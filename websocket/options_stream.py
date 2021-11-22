import polygon
from polygon import enums
import datetime
from datetime import datetime, timezone
import time
import threading
import config
import traceback
import requests
import redis
import json
from polygon import build_option_symbol, parse_option_symbol

print("starting stream...")
key = config.polygon_key


def connections():
    # redis_pool = redis.ConnectionPool(host=config.redis_host, port=config.redis_port, db=0, password=config.redis_pw)
    redis_pool = redis.ConnectionPool(connection_class=redis.UnixDomainSocketConnection, path="/var/run/redis/redis-server.sock",
                                      password=config.redis_pw, db=0)
    r = redis.Redis(connection_pool=redis_pool, charset="utf-8", decode_responses=True)
    print('redis connected', r)
    return r


def redis_message(messages):
    for message in messages:
        r.rpush('options-trades-list', json.dumps(message))
    return None


def unix_convert(ts):
    ts = int(ts/1000)
    tdate = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return tdate


def save_data(message):
    z = time.time()
    print(message)
    # initializing strings
    option_symbol = message[0]['sym']
    parsed_details = parse_option_symbol(option_symbol)
    expiration = parsed_details.expiry
    underlying_symbol = parsed_details.underlying_symbol
    strike = parsed_details.strike_price
    option_type = parsed_details.call_or_put
    postfix_symbol = build_option_symbol(underlying_symbol, expiration, option_type, strike)

    str_time = expiration.strftime('%Y-%m-%d') + ' 20:00:00'
    keys = {'sym': 'symbol', 'x': 'exch_id', 'p': 'price', 's': 'trade_size', 'c': 'trade_conditions',
            't': 'trade_time', 'q': 'sequence'}

    # Drop Unknown Keys
    key_count = len(message[0].keys())
    if key_count > len(keys.keys())+1:
        message = [{k: single[k] for k in keys if k in single} for single in message]
        print('New fields detected! Check API documentation: https://polygon.io/docs/websockets/ws_options_T_anchor')
    else:
        message = [{k: single[k] for k in keys if k in single} for single in message]
        
    new_message = []
    for d in message:
        # del d['ev']  # delete status
        d = {keys[name]: val for name, val in d.items()}
        trade_conditions = str(d['trade_conditions'])
        d['trade_conditions'] = trade_conditions.replace('[', '').replace(']', '')
        d['underlying_symbol'] = underlying_symbol
        d['strike'] = int(strike) / 1000
        d['option_type'] = option_type
        d['symbol'] = postfix_symbol
        d['expiration'] = str_time
        d['expiration_timestamp'] = int(datetime.strptime(str_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())*1000
        d['tdate'] = unix_convert(d['trade_time'])
        d['save_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        cols = ['underlying_symbol', 'symbol', 'strike', 'option_type', 'expiration', 'expiration_timestamp', 'price',
                'trade_size', 'trade_time', 'exch_id', 'trade_conditions', 'tdate', 'save_date']
        d = {k: d[k] for k in cols}
        new_message.append(d)
    # redis_message(new_message)
    print(message)
    print(time.time()-z)
    return None


def my_custom_process_message(ws, msg):
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
    my_client = polygon.StreamClient(key, polygon.enums.StreamCluster('options'), on_message=my_custom_process_message,
                                     on_close=my_custom_close_handler, on_error=my_custom_error_handler)
    # my_client = polygon.StreamClient(key, polygon.enums.StreamCluster('crypto'), on_message=my_custom_process_message,
    #                                  on_close=my_custom_close_handler, on_error=my_custom_error_handler)
    try:
        my_client.start_stream_thread()
        my_client.subscribe_option_trades()
        # my_client.subscribe_crypto_minute_aggregates()
        # my_client.subscribe_stock_trades()
    except Exception:
        traceback.print_exc()
        my_client.unsubscribe_option_trades()
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


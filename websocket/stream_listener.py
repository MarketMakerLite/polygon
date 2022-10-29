import datetime
import os
import redis
import time
# import psycopg2
# from psycopg2 import pool
import json
import io
import csv
import config
import threading
import traceback

"""Use this function if you're using TCP/IP sockets or if you are not sure how you're connecting"""
redis_pool = redis.ConnectionPool(host=config.redis_host, port=config.redis_port, db=0, password=config.redis_pw)

"""Use this function if you're using UnixDomainSockets"""
# redis_pool = redis.ConnectionPool(connection_class=redis.UnixDomainSocketConnection, path="/var/run/redis/redis-server.sock",
#                                   password=config.redis_pw, db=0)

r = redis.Redis(connection_pool=redis_pool, charset="utf-8", decode_responses=True)

print('redis connected', redis_pool)
postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(5, 50, host=config.psql_host, database=config.psql_db,
                                                       user=config.psql_user, password=config.psql_pw)
print('postgres connected', postgreSQL_pool)

stock_list = 'stock-list'
options_agg_list = 'options-agg-list'
options_trades = 'options-trades-list'
crypto_agg_list = 'crypto-list'

stock_cols = ['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open', 'tick_close',
              'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date']
option_cols = ['underlying_symbol', 'symbol', 'strike', 'option_type', 'expiration', 'expiration_timestamp', 'price',
               'trade_size', 'trade_time', 'exch_id', 'trade_conditions', 'tdate', 'save_date', 'sequence']
crypto_agg_cols = ['pair', 'tick_volume', 'tick_vwap', 'avg_trade_size', 'tick_open', 'tick_close', 'tick_high',
                   'tick_low', 'time_beg', 'time_end', 'tdate', 'save_date']

stock_info = {'cols': stock_cols, 'table': 'stockdata'}
option_info = {'cols': option_cols, 'table': 'optionstrades'}
crypto_info = {'cols': crypto_agg_cols, 'table': 'crypto_min'}


def sql_fun(messages, info):
    z = time.time()
    conn = postgreSQL_pool.getconn()
    conn.autocommit = True
    cur = conn.cursor()
    sio = io.StringIO()
    writer = csv.writer(sio)
    values = [[value for value in message.values()] for message in messages]
    writer.writerows(values)
    sio.seek(0)
    cur.copy_from(file=sio, table=info['table'], sep=",", columns=info['cols'], null='')
    cur.close()
    postgreSQL_pool.putconn(conn)
    print(datetime.datetime.utcnow(), info['table'], time.time()-z)
    return None


def get_messages(q_name, prefetch_count=10000000000):
    pipe = r.pipeline()
    pipe.lrange(q_name, 0, prefetch_count - 1)  # Get msgs (w/o pop)
    pipe.ltrim(q_name, prefetch_count, -1)  # Trim (pop) list to new value
    messages, trim_success = pipe.execute()
    return messages


def saver(messages, info):
    if messages:
        msg = [json.loads(message) for message in messages]
        threading.Thread(target=sql_fun, args=[msg, info]).start()
        return None


print('starting listening client')
try:
    while True:
        """Get Messages"""
        messages_stocks = get_messages(stock_list)
        messages_options = get_messages(options_trades)
        messages_crypto = get_messages(crypto_agg_list)
        """Write to Database"""
        saver(messages_stocks, stock_info)
        saver(messages_options, option_info)
        saver(messages_crypto, crypto_info)
        """Heartbeat"""
        time.sleep(0.9)

except Exception:
    traceback.print_exc()
    postgreSQL_pool.closeall()


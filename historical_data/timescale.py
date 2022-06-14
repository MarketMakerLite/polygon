import traceback
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2 import pool
from datetime import datetime
import time
import config
from polygon import StocksClient
from typing import Iterator, Optional
import io
import asyncio
import csv
import json
from itertools import dropwhile
# import uvloop  # Unix only

stock_cols = ['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open', 'tick_close', 'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date']
engine = create_engine(config.psql)
postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(5, 50, host=config.psql_host, database=config.psql_db, user=config.psql_user, password=config.psql_pw)
key = config.polygon_key
stocks_client = StocksClient(key, True, read_timeout=60)     # Async client
symbols_df = pd.read_sql_query('select ticker from companies where active = true', con=engine)
symbols = symbols_df['ticker'].to_list()

# Restart at a specific ticker
# elem = 'EEV'
# symbols = list(dropwhile(lambda x: x != elem, symbols))

print(symbols)


class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)


def unix_convert(ts):
    ts = int(ts/1000)
    tdate = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return tdate


def clean_csv_value(value):
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')


def sql_fun(result, size=1024):
    conn = postgreSQL_pool.getconn()
    conn.autocommit = True
    cur = conn.cursor()
    sio = StringIteratorIO((
        '|'.join(map(clean_csv_value, (
            row['symbol'],
            row['tick_volume'],
            row['total_volume'],
            row['opening_price'],
            row['tick_vwap'],
            row['tick_open'],
            row['tick_close'],
            row['tick_high'],
            row['tick_low'],
            row['vwap'],
            row['avg_trade_size'],
            row['time_beg'],
            row['time_end'],
            row['tdate'],
            row['save_date'],
        ))) + '\n'
        for row in result
    ))
    cur.copy_from(sio, 'stockdata_hist', sep='|', size=size)
    cur.close()
    postgreSQL_pool.putconn(conn)
    return None


async def get_ticker_data(ticker: str):
    # Make API Call
    resp = await stocks_client.get_aggregate_bars(ticker, '2001-01-01', '2022-06-12', full_range=True, timespan='minute', high_volatility=True, warnings=False, adjusted=True)
    for d in resp:
        d.setdefault('a')
        d.setdefault('op')
        d.setdefault('n')
        d.setdefault('vw')
        if 'a' in d:
            del d['a']
        if 'n' in d:
            del d['n']
    resp = [{'v': d['v'], 'vw': d['vw'], 'o': d['o'], 'c': d['c'], 'h': d['h'], 'l': d['l'], 't': d['t'], 'op': d['op']} for d in resp]
    for d in resp:
        d['tick_volume'] = d.pop('v')
        d['tick_vwap'] = d.pop('vw')
        d['tick_open'] = d.pop('o')
        d['tick_close'] = d.pop('c')
        d['tick_high'] = d.pop('h')
        d['tick_low'] = d.pop('l')
        d['time_end'] = d.pop('t')
        d['opening_price'] = d.pop('op')
        d['symbol'] = ticker
        d['total_volume'] = None
        d['vwap'] = None
        d['avg_trade_size'] = None
        d['time_beg'] = None
        d['tick_volume'] = int(d['tick_volume'])
        d['tdate'] = unix_convert(d['time_end'])
        d['save_date'] = datetime.utcnow()
    resp = [{'symbol': d['symbol'], 'tick_volume': d['tick_volume'], 'total_volume': d['total_volume'], 'opening_price': d['opening_price'],
             'tick_vwap': d['tick_vwap'], 'tick_open': d['tick_open'], 'tick_close': d['tick_close'], 'tick_high': d['tick_high'],
             'tick_low': d['tick_low'], 'vwap': d['vwap'], 'avg_trade_size': d['avg_trade_size'], 'time_beg': d['time_beg'],
             'time_end': d['time_end'], 'tdate': d['tdate'], 'save_date': d['save_date']} for d in resp]
    return resp


# def prep_database():
#     print("Preparing Database")
#     refresh_table = text("DELETE FROM stockdata_hist;")
#     with engine.connect() as conn:
#         conn.execute(refresh_table)


async def main():
    print(f"Getting data for {len(symbols)} symbols")
    counter = 0
    times_list = []
    table_name = 'stockdata_hist'
    job_id = pd.read_sql_query(f"""SELECT s.job_id
            FROM timescaledb_information.jobs j
            INNER JOIN timescaledb_information.job_stats s ON j.job_id = s.job_id
            WHERE j.proc_name = 'policy_compression' AND s.hypertable_name = '{table_name}'; """, con=engine)
    job_id = job_id['job_id'][0]
    print(job_id)
    for ticker in symbols:
        counter = counter + 1
        print(f'{counter}/{len(symbols)}')
        try:
            t = time.time()
            print(f"Getting data for {ticker}")
            result = await get_ticker_data(ticker)
            print("Time to data:", time.time()-t)
            # Save to database
            print(f"Saving {ticker} to database")
            t2 = time.time()
            sql_fun(result)
            print("sql time:", time.time()-t2)
            times_list.append(time.time() - t)
            print(ticker, (sum(times_list) / len(times_list)))  # Time it
            if counter % 2000 == 0:
                print('Running compression')
                conn = postgreSQL_pool.getconn()
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute(f"""CALL run_job({job_id});""")
                cur.close()
                postgreSQL_pool.putconn(conn)
                print('Sleeping...')

        except Exception as e:
            print(e)
            traceback.print_exc()
            pass


if __name__ == '__main__':
    # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # Unix only
    # prep_database()
    asyncio.run(main())
    stocks_client.close()

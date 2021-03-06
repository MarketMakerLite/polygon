import traceback
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, date, timedelta
import time
import config
from polygon import StocksClient
from typing import Iterator, Optional
import io
import asyncio
import asyncpg
import json
from itertools import dropwhile
# import uvloop  # Unix only

stock_cols = ['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open', 'tick_close', 'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date']
engine = create_engine(config.psql)
key = config.polygon_key
stocks_client = StocksClient(key, True, read_timeout=60)     # Async client
symbols_df = pd.read_sql_query('select ticker from companies where active = true', con=engine)
symbols = symbols_df['ticker'].to_list()

# Restart at a specific ticker
# elem = 'EEV'
# symbols = list(dropwhile(lambda x: x != elem, symbols))
print(symbols)


def unix_convert(ts):
    if len(str(ts)) == 13:
        ts = int(ts/1000)
        tdate = datetime.utcfromtimestamp(ts)  # .strftime('%Y-%m-%d %H:%M:%S')
        return tdate
    if len(str(ts)) == 19:
        ts = int(ts/1000000000)
        tdate = datetime.utcfromtimestamp(ts)  # .strftime('%Y-%m-%d %H:%M:%S')
        return tdate


async def sql_fun(data, conn):
    sio = [tuple(d.values()) for d in data]
    await conn.copy_records_to_table('stockdata_hist', records=sio, columns=stock_cols)
    del sio


async def get_ticker_data(ticker: str, start_date, end_date):
    # Make API Call
    resp = await stocks_client.get_aggregate_bars(ticker, start_date, end_date, full_range=True, timespan='minute', high_volatility=True, warnings=False, adjusted=True)
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


async def main():
    pool = await asyncpg.create_pool(host=config.psql_host, database=config.psql_db, user=config.psql_user, password=config.psql_pw)
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
    conn = await pool.acquire()
    await conn.execute(f"""SELECT alter_job({job_id}, scheduled => false);""")
    await pool.release(conn)
    list_len = 20
    symbols_list = [symbols[x:x + list_len] for x in range(0, len(symbols), list_len)]
    start_date = date(2003, 1, 1)
    end_date = date(2022, 6, 16)
    subset = 2
    end_date1 = start_date + (end_date - start_date) / subset
    for i in range(1, subset):
        print(start_date, end_date1)
        try:
            for symbol in symbols_list:
                df = []
                for ticker in symbol:
                    print(f'{counter}/{len(symbols)}')
                    try:
                        # Get Data for a Group of Tickers
                        t = time.time()
                        counter = counter + 1
                        t3 = time.time()
                        print(f"Getting data for {ticker}")
                        new = await get_ticker_data(ticker, start_date, end_date1)
                        print("Time to data:", time.time()-t3)
                        df = df + new
                        times_list.append((time.time() - t))
                    except Exception as e:
                        print(e)
                        traceback.print_exc()
                        pass
                # Save to database
                print(f"Saving to database")
                t2 = time.time()
                conn = await pool.acquire()
                await sql_fun(df, conn)
                await pool.release(conn)
                print("sql time: ", time.time()-t2)
                print("Average: ", (sum(times_list) / len(times_list)))  # Time it
            print('Updating dates')
            print(start_date)
            start_date = end_date1 + timedelta(days=1)
            end_date1 = end_date
            print(start_date, end_date1)
            print('Running compression')
            conn = await pool.acquire()
            await conn.execute(f"""CALL run_job({job_id});""")
            await pool.release(conn)
        except Exception as e:
            print(e)
            traceback.print_exc()
            pass
    print("Turning On Compression Policy")
    conn = await pool.acquire()
    await conn.execute(f"""SELECT alter_job({job_id}, scheduled => true);""")
    await pool.release(conn)
    return


if __name__ == '__main__':
    # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # Unix only
    asyncio.run(main())
    stocks_client.close()

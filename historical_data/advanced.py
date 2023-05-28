import traceback
# from csv import writer
from csv import DictWriter
# import pandas as pd
# from sqlalchemy import create_engine, text
from datetime import datetime, date, timedelta
import time
import config
from datetime import timezone
from polygon import StocksClient
from typing import Iterator, Optional
import io
import asyncio
# import asyncpg
import json
from itertools import dropwhile
# import uvloop  # Unix only

stock_cols = ['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open', 'tick_close', 'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date']
# engine = create_engine(config.psql)
key = config.polygon_key
stocks_client = StocksClient(api_key=key, use_async=True, read_timeout=60)     # Async client
# symbols_df = pd.read_sql_query('select ticker from companies where active = true', con=engine)
# symbols = symbols_df['ticker'].to_list()
symbols = ['TSLA', 'AMZN']

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

async def get_ticker_data(ticker: str, start_date, end_date):
    # Make API Call
    # Note that while this is calling the routine with await, it is also running parallel threads to pull down
    # data on lots of network connections. It's unclear the extent to which this will actually yield to other
    # coroutines during its work. It may be that this operation alone takes all the resources of the machine
    # making "await" functionally useless

    # start = time.time()
    print(f"{time.time()}: get_ticker_data: start getting aggregate bars for {ticker}")
    resp = await stocks_client.get_aggregate_bars(symbol=ticker
                                                  , from_date=start_date, to_date=end_date, timespan='minute'
                                                  , full_range=True, run_parallel=True, max_concurrent_workers=8
                                                  , high_volatility=True, warnings=False, adjusted=True)
    print(f"{time.time()}: get_ticker_data: done getting aggregate bars for {ticker}")

    for d in resp:
        d.setdefault('a')
        d.setdefault('op')
        d.setdefault('n')
        d.setdefault('vw')
        if 'a' in d:
            del d['a']
        if 'n' in d:
            del d['n']
    resp = [{'v': d['v'], 'vw': d['vw'], 'o': d['o'], 'c': d['c'], 'h': d['h'], 'l': d['l'], 't': d['t'], 'op': d['op']}
            for d in resp]
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
        d['save_date'] = datetime.now(timezone.utc)
    resp = [{ 'symbol': d['symbol'], 'tick_volume': d['tick_volume'], 'total_volume': d['total_volume']
            , 'opening_price': d['opening_price'], 'tick_vwap': d['tick_vwap'], 'tick_open': d['tick_open']
            , 'tick_close': d['tick_close'], 'tick_high': d['tick_high'], 'tick_low': d['tick_low']
            , 'vwap': d['vwap'], 'avg_trade_size': d['avg_trade_size'], 'time_beg': d['time_beg']
            , 'time_end': d['time_end'], 'tdate': d['tdate'], 'save_date': d['save_date']} for d in resp]
    print(f"{time.time()}: get_ticker_data: done formatting aggregate bars dictionary response for {ticker}")

    return resp


async def main():
    # pool = await asyncpg.create_pool(host=config.psql_host, database=config.psql_db, user=config.psql_user, password=config.psql_pw)
    print(f"Getting data for {len(symbols)} symbols")
    counter = 0
    times_list = []

    # table_name = 'stockdata_hist'
    # job_id = pd.read_sql_query(f"""SELECT s.job_id
    #         FROM timescaledb_information.jobs j
    #         INNER JOIN timescaledb_information.job_stats s ON j.job_id = s.job_id
    #         WHERE j.proc_name = 'policy_compression' AND s.hypertable_name = '{table_name}'; """, con=engine)
    # job_id = job_id['job_id'][0]
    # print(job_id)
    # conn = await pool.acquire()
    # await conn.execute(f"""SELECT alter_job({job_id}, scheduled => false);""")
    # await pool.release(conn)

    list_len = 20
    symbols_list = [symbols[x:x + list_len] for x in range(0, len(symbols), list_len)]
    # start_date = date(2003, 1, 1)
    start_date = date(2021, 1, 1)
    end_date = date(2021, 12, 31)
    subset = 2
    end_date1 = start_date + (end_date - start_date) / subset

    field_names = ['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open',
                   'tick_close', 'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg',
                   'time_end', 'tdate', 'save_date']

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

                        with open(f'C:\Repos\polygon_nautilus\.data\{ticker}.csv', 'a') as f_object:
                            dictwriter_object = DictWriter(f_object, fieldnames=field_names)
                            for d in new:
                                dictwriter_object.writerow(d)
                            f_object.close()

                        # df = df + new
                        times_list.append((time.time() - t))
                    except Exception as e:
                        print(e)
                        traceback.print_exc()
                        pass
                # Save to database
                print(f"Saving to csv")
                t2 = time.time()

               # sio = [tuple(d.values()) for d in df]

                # with open(f'C:\Repos\polygon_nautilus\data\{ticker}.csv', 'a') as f_object:
                    # Pass the file object and a list
                    # of column names to DictWriter()
                    # You will get a object of DictWriter
                    # dictwriter_object = DictWriter(f_object, fieldnames=field_names)
                    # dictwriter_object = DictWriter(f_object)

                    # Pass the dictionary as an argument to the Writerow()
                    # for d in df
                    #     dictwriter_object.writerow(d)

                    # Close the file object
                    # f_object.close()

                # sio
                # del sio

                """
                conn = await pool.acquire()
                await sql_fun(df, conn)
                await pool.release(conn)
                """
                print("sql time: ", time.time()-t2)
                print("Average: ", (sum(times_list) / len(times_list)))  # Time it
            print('Updating dates')
            print(start_date)
            start_date = end_date1 + timedelta(days=1)
            end_date1 = end_date
            print(start_date, end_date1)
            # print('Running compression')

            # conn = await pool.acquire()
            # await conn.execute(f"""CALL run_job({job_id});""")
            # await pool.release(conn)

        except Exception as e:
            print(e)
            traceback.print_exc()
            pass
    # print("Turning On Compression Policy")

    # conn = await pool.acquire()
    # await conn.execute(f"""SELECT alter_job({job_id}, scheduled => true);""")
    # await pool.release(conn)
    await stocks_client.close()

    return


if __name__ == '__main__':
    # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # Unix only
    asyncio.run(main())

    # loop.run_until_complete(loop.create_task(stocks_client.close()))
    # loop.run_until_complete(self.loop.create_task(self._websocket.close()))

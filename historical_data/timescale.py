import traceback
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2 import pool
from datetime import datetime
import time
import config
from polygon import StocksClient
import io
import asyncio
from itertools import dropwhile
# import uvloop  # Unix only

stock_cols = ['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open', 'tick_close', 'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date']
engine = create_engine(config.psql)
postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(5, 50, host=config.psql_host, database=config.psql_db, user=config.psql_user, password=config.psql_pw)
key = config.polygon_key
stocks_client = StocksClient(key, True)     # Async client
symbols_df = pd.read_sql_query('select ticker from companies where active = true', con=engine)
symbols = symbols_df['ticker'].to_list()

# Restart at a specific ticker
# elem = 'EEV'
# symbols = list(dropwhile(lambda x: x != elem, symbols))
# print(symbols)


def unix_convert(ts):
    ts = int(ts/1000)
    tdate = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return tdate


def sql_fun(df):
    conn = postgreSQL_pool.getconn()
    conn.autocommit = True
    cur = conn.cursor()
    sio = io.StringIO()
    df.to_csv(sio, index=False, header=False)
    del df
    sio.seek(0)
    cur.copy_from(file=sio, table='stockdata_hist', sep=",", columns=stock_cols, null='')
    cur.close()
    postgreSQL_pool.putconn(conn)
    del sio


async def get_ticker_data(ticker: str):
    # Make API Call
    resp = await stocks_client.get_aggregate_bars(ticker, '2000-01-01', '2022-05-13', full_range=True, timespan='minute',
                                                  high_volatility=True, warnings=False, adjusted=True)
    df = pd.DataFrame.from_dict(resp)
    # Formatting
    if 'a' in df.columns:
        df = df[['v', 'a', 'vw', 'o', 'c', 'h', 'l', 't', 'n', 'op']]
    else:
        df.insert(1, 'a', None)
        df.insert(9, 'op', None)
        df = df[['v', 'a', 'vw', 'o', 'c', 'h', 'l', 't', 'n', 'op']]
    df.drop('a', axis=1, inplace=True)
    df.drop('n', axis=1, inplace=True)
    df.columns = ['tick_volume', 'tick_vwap', 'tick_open', 'tick_close', 'tick_high', 'tick_low', 'time_end', 'opening_price']
    df['symbol'] = ticker
    df['total_volume'] = None
    df['vwap'] = None
    df['avg_trade_size'] = None
    df['time_beg'] = None
    df['tick_volume'] = df['tick_volume'].astype('int')
    df['tdate'] = df['time_end'].map(lambda x: unix_convert(x))
    df['save_date'] = datetime.utcnow()
    df = df[['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open', 'tick_close',
             'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date']]
    return df


def prep_database():
    print("Preparing Database")
    refresh_table = text("DELETE FROM stockdata_hist;")
    with engine.connect() as conn:
        conn.execute(refresh_table)


async def main():
    print(f"Getting data for {len(symbols)} symbols")
    counter = 0
    times_list = []
    table_name = 'stockdata_hist'
    job_id = pd.read_sql_query(f"""SELECT s.job_id  # Get compression policy job id (https://docs.timescale.com/timescaledb/latest/how-to-guides/compression/about-compression)
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
            df = await get_ticker_data(ticker)
            # Save to database
            print(f"Saving {ticker} to database")
            sql_fun(df)
            times_list.append(time.time() - t)
            print(ticker, (sum(times_list) / len(times_list)))  # Time it
            if counter % 1000 == 0:  # Compress every 1,000 tickers, roughly 400gb for the full history
                print('Running compression')
                conn = postgreSQL_pool.getconn()
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute(f"""CALL run_job({job_id});""")
                cur.close()
                conn.close()
        except Exception as e:
            print(e)
            traceback.print_exc()
            pass


if __name__ == '__main__':
    # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # Unix only
    prep_database()
    asyncio.run(main())
    stocks_client.close()

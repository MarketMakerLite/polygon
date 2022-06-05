import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2 import pool
from datetime import datetime, timedelta
import time
import config
import polygon
from polygon import StocksClient
import io
import asyncio
# import uvloop  # Unix only


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
    sio.seek(0)
    cur.copy_from(file=sio, table='stockdata_hist', sep=",", columns=stock_cols, null='')
    cur.close()
    postgreSQL_pool.putconn(conn)


async def get_ticker_data(ticker):
    # Make API Call
    today = datetime.today().date()
    resp = await stocks_client.get_aggregate_bars(ticker, '2005-01-01', today, full_range=True, timespan='minute',
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


async def main(symbol_list):
    print(f"Getting data for {len(symbol_list)} symbols")
    # Get compression Job ID
    job_id = pd.read_sql_query(f"""SELECT s.job_id
                                       FROM timescaledb_information.jobs j
                                       INNER JOIN timescaledb_information.job_stats s ON j.job_id = s.job_id
                                       WHERE j.proc_name = 'policy_compression' AND s.hypertable_name = 'stockdata_hist'; """,
                               con=engine)
    job_id = job_id['job_id'][0]
    # Turn off compression (Job ID from previous step)
    print('Turning off Compression Policy')
    turn_off_compression = text(f"""SELECT alter_job({job_id}, scheduled => false);""")
    with engine.connect() as conn:
        conn.execute(turn_off_compression)
    for ticker in symbol_list:
        print(f"Getting data for {ticker}")
        try:
            df = await get_ticker_data(ticker)
            # Save to database
            print(f'Decompressing Data for {ticker}')
            get_chunk_ids = pd.read_sql_query(
                f"""SELECT tableoid::regclass FROM stockdata_hist WHERE symbol = '{ticker}' GROUP BY tableoid; """,
                con=engine)
            chunk_ids = get_chunk_ids['tableoid'].to_list()
            for chunk_id in chunk_ids:
                decompress_chunks = text(f"""SELECT decompress_chunk('{chunk_id}');""")
                with engine.connect() as conn:
                    conn.execute(decompress_chunks)
            # Add new data
            print('Adding new data to table')
            sql_fun(df)
            # Restart compression
            print(f'Successfully updated {ticker}')
        except Exception as e:
            print(e)
            pass
    print('Restarting compression policy')
    restart_compression = text(f"""SELECT alter_job({job_id}, scheduled => true);""")
    run_compression = text(f"""CALL run_job({job_id});""")
    with engine.connect() as conn:
        conn.execute(restart_compression)
        conn.execute(run_compression)
    return None


def stock_splits(symbol_list):
    splits_list = []
    execution_date = datetime(2022, 5, 10).date()
    for ticker in symbol_list:
        print(f"Checking {ticker} for splits since {execution_date}")
        resp = reference_client.get_stock_splits(ticker, all_pages=True)
        if resp:
            if resp[0]['ticker']:
                for split in resp:
                    split_date = datetime.strptime(split['execution_date'], "%Y-%m-%d").date()
                    if datetime.today().date() >= split_date >= execution_date:
                        splits_list.append(resp[0]['ticker'])
                        print(splits_list)
    return splits_list


if __name__ == '__main__':
    start = time.time()
    # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # Unix only
    engine = create_engine(config.psql)
    postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(5, 50, host=config.psql_host, database=config.psql_db,
                                                           user=config.psql_user, password=config.psql_pw)
    reference_client = polygon.ReferenceClient(config.polygon_key, False, read_timeout=60)
    symbols_df = pd.read_sql_query('select ticker from companies where active = true', con=engine)
    symbols = symbols_df['ticker'].to_list()
    stock_cols = ['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open', 'tick_close',
                  'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date']
    # symbols = ['BAOS', 'BEST', 'BHAT', 'BQ', 'BRW', 'BZQ', 'CIG', 'CIG.C', 'CM', 'CWEB', 'DIG', 'ERY', 'EVOK', 'FAMI', 'FENG', 'HIVE', 'HIVE', 'KOLD', 'LEJU', 'MKD', 'PT', 'PTE', 'PXS', 'RMTI', 'SAL', 'SCC', 'SCO', 'SRGA', 'SXTC', 'TNXP', 'UCO', 'UYM', 'YCS', 'YINN']
    splits_list = stock_splits(symbols)
    print(splits_list)
    reference_client.close()
    print("Time:", (time.time()-start))
    stocks_client = StocksClient(config.polygon_key, True)
    asyncio.run(main(splits_list))
    stocks_client.close()

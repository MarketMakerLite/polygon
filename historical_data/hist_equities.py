import pandas as pd
# from sqlalchemy import create_engine, text
# import psycopg2
# from psycopg2 import pool
from datetime import datetime
import time
import config
from polygon import StocksClient
import io
# import uvloop  # Unix only

# engine = create_engine(config.psql)
# postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(5, 50, host=config.psql_host, database=config.psql_db,
#                  user=config.psql_user, password=config.psql_pw)
key = config.polygon_key
stocks_client = StocksClient(key, True)

#my_client = polygon.StreamClient(key, polygon.enums.StreamCluster('stocks'), host='delayed.polygon.io'
#                                 , on_message=my_custom_process_message, on_close=my_custom_close_handler
#                                 , on_error=my_custom_error_handler)

# symbols_df = pd.read_sql_query('select ticker from companies where active = true', con=engine)
# symbols = symbols_df['ticker'].to_list()
symbols = ['SPCE']
stock_cols = ['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open', 'tick_close',
              'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date']


def unix_convert(ts):
    ts = int(ts/1000)
    tdate = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return tdate

"""
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
"""

async def get_ticker_data(ticker: str):
    # Make API Call
    # resp = await stocks_client.get_aggregate_bars(ticker, '2005-01-01', '2022-05-10', full_range=True, timespan='minute',
    resp = await stocks_client.get_aggregate_bars(ticker, '2022-05-03', '2022-05-04', full_range=True, timespan='minute',
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

"""
def prep_database():
    print("Preparing Database")
    refresh_table = text("DELETE FROM stockdata_hist;")
    with engine.connect() as conn:
        conn.execute(refresh_table)
"""

async def main():
    print(f"Getting data for {len(symbols)} symbols")
    times_list = []
    for ticker in symbols:
        try:
            t = time.time()
            df = await get_ticker_data(ticker)
            # Save to database
            #sql_fun(df)
            #times_list.append(time.time() - t)
            print(ticker, (sum(times_list) / len(times_list)))  # Time it
        except Exception:
            pass

    stocks_client.close()

    return

if __name__ == '__main__':
    import asyncio
    # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # Unix only
    # prep_database()
    asyncio.run(main())


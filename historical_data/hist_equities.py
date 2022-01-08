import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date, datetime, timedelta
import concurrent.futures
import requests as rq
import time
import config
import traceback
pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)
# Key
key = config.polygon_key

# Input
multiplier = 1
timespan = 'minute'
adjusted = 'true'
sort = 'desc'
limit = 50000

# Database / Get list of tickers
engine = create_engine(config.psql)
symbols_df = pd.read_sql_query('select ticker from companies where active = true', con=engine)
symbols = symbols_df['ticker'].to_list()
print(symbols)


class PolygonClient:
    def __init__(self):
        self.KEY = key  # your key
        self.BASE = 'https://api.polygon.io'
        self.session = rq.session()
        self.session.headers.update({'Authorization': 'Bearer %s' % self.KEY})

    def get_response(self, path: str, params: dict = None):
        return self.session.request('GET', self.BASE + path, params=params).json()

    def get_next_page(self, url: str):
        return self.session.request('GET', url).json()


def unix_convert(ts):
    ts = int(ts/1000)
    tdate = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return tdate


def call_api(symbol, start_date, end_date):
    try:
        pg_client = PolygonClient()
        endpoint = '/v2/aggs/ticker/%s/range/%d/%s/%s/%s?adjusted=%s&sort=%s&limit=%d'
        resp = pg_client.get_response(
            endpoint % (symbol, multiplier, timespan, start_date, end_date, adjusted, sort, limit))
        if resp == 'results':
            df = None
        else:
            if 'results' in resp:
                df = pd.DataFrame.from_dict(resp['results'])

                if 'a' in df.columns:
                    df = df[['v', 'a', 'vw', 'o', 'c', 'h', 'l', 't', 'n', 'op']]
                    pass
                else:
                    df.insert(1, 'a', None)
                    df.insert(9, 'op', None)
                    df = df[['v', 'a', 'vw', 'o', 'c', 'h', 'l', 't', 'n', 'op']]

                df.columns = ['tick_volume', 'a', 'tick_vwap', 'tick_open', 'tick_close', 'tick_high', 'tick_low',
                              'time_end', 'n', 'opening_price']
                df.insert(0, 'symbol', symbol)
                df.insert(3, 'total_volume', None)
                df.insert(3, 'vwap', None)
                df.insert(3, 'avg_trade_size', None)
                df.insert(3, 'time_beg', None)
                df.drop('a', axis=1, inplace=True)
                df.drop('n', axis=1, inplace=True)
                df['tdate'] = df['time_end'].map(lambda x: unix_convert(x))
                df['save_date'] = datetime.utcnow()
                df = df[['symbol', 'tick_volume', 'total_volume', 'opening_price',
                         'tick_vwap', 'tick_open','tick_close', 'tick_high', 'tick_low',
                         'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date']]
            else:
                df = None
    except Exception as exe:
        print(exe)
        traceback.print_exc()
        df = 'results'
    return df


def get_data(symbol):
    start_time2 = time.time()
    # Date Info
    from_date = '2015-01-01'
    from_date = datetime.strptime(from_date, '%Y-%m-%d').date()
    to_date = datetime.today().date() + timedelta(days=1)
    # Transform Dates
    temp_date_start = to_date - timedelta(days=60)
    temp_date_end = to_date
    print(f'getting data for {symbol}')
    while temp_date_start > from_date:
        temp_df = call_api(symbol, temp_date_start, temp_date_end)
        temp_date_end = temp_date_end - timedelta(days=60)
        temp_date_start = temp_date_start - timedelta(days=60)
        if isinstance(type(temp_df), str):
            break
        else:
            # Add df to database
            #print(temp_df)
            temp_df.to_sql('stockdata_hist', engine, if_exists='append', index=False,  method='multi')
    print(time.time() - start_time2)


def main():
    # Format database and empty previous values
    df = pd.DataFrame([['delete_this_row', 0, 0, 0, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0, 0, 0, datetime.today(), datetime.today()]],
                      columns=['symbol', 'tick_volume',
                               'total_volume', 'opening_price', 'tick_vwap', 'tick_open', 'tick_close', 'tick_high',
                               'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date'])

    # Backup table
    print("backing up table")
    # df.to_sql('stockdata_backup', engine, if_exists='replace', index=False)
    # query1 = text("DELETE FROM stockdata_backup WHERE symbol = 'delete_this_row';")
    query1 = text("DROP TABLE IF EXISTS stockdata_backup;")
    query2 = text("create table stockdata_backup as (select * from stockdata_hist);")
    with engine.connect() as conn:
        conn.execute(query1)
        conn.execute(query2)

    #Create main table
    df.to_sql('stockdata_hist', engine, if_exists='replace', index=False)
    query2 = text("DELETE FROM stockdata_hist WHERE symbol = 'delete_this_row';")
    with engine.connect() as conn:
        conn.execute(query2)

    # Get new data
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(get_data, symbols)

    query3 = text("CREATE INDEX idx_sdh_tdate ON stockdata_hist (tdate);")
    query4 = text("CREATE INDEX idx_sdh_symbol ON stockdata_hist (symbol);")
    with engine.connect() as conn:
        conn.execute(query3)
        conn.execute(query4)


if __name__ == '__main__':
    start_time = time.time()
    main()
    print(time.time() - start_time)
    time_hr = (time.time() - start_time)/3600
    print(time_hr, " hrs")

from datetime import datetime, date, timedelta
import time
import config
from polygon import StocksClient
from polygon import ReferenceClient
import asyncio
from nautilus_trader.model.currencies import USD
import nautilus_trader.model.identifiers as nti
from nautilus_trader.model.instruments.equity import Equity
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from threading import Thread
from queue import Queue

assert isinstance(USD, object)


def ticker_factory(key):
    print("Retrieving tickers")
    symbols = ['TSLA', 'AMZN']  # used to filter results for testing
    ref_client = ReferenceClient(key, use_async=False)

    try:
        tickers = ref_client.get_tickers(market='stocks', all_pages=True, merge_all_pages=True)
        # filter here if you are just testing
        tickers = [d for d in tickers if d['ticker'] in symbols]
        print("Tickers retrieved")
        return tickers
    finally:
        ref_client.close()


"""
# producer task
def producer(queue):
    print('Producer: Running')
    # generate items
    for i in range(10):
        # generate a value
        value = random()
        # block, to simulate effort
        sleep(value)
        # create a tuple
        item = (i, value)
        # add to the queue
        queue.put(item)
        # report progress
        print(f'>producer added {item}')
    # signal that there are no further items
    queue.put(None)
    print('Producer: Done')
"""


def create_equity_instrument(ticker_symbol: str, ticker_venue: nti.Venue, currency, cik) -> Equity:
    return Equity(
        instrument_id=nti.InstrumentId(nti.Symbol(ticker_symbol), ticker_venue),
        native_symbol=nti.Symbol(ticker_symbol),
        currency=currency,
        price_precision=2,
        price_increment=Price.from_str('0.01'),
        multiplier=Quantity.from_int(1),
        lot_size=Quantity.from_int(1),
        ts_event=0,
        ts_init=0,
        isin=cik,  # we should be converting this to isin...
    )


def produce_instrument(instrument_queue, ticker_list):
    print("Begin instrument production")

    for t in ticker_list:
        print(f"Producer is adding instrument for {t['ticker']} to the queue")
        yield(create_equity_instrument(t['ticker'], nti.Venue(t['primary_exchange']), USD, t['cik']))

        # print(f"Producer is yielding instrument for {row.ticker}")
        # yield generic_equity(t['ticker'], t['primary_exchange'], t['currency_name'], t['cik'])

    instrument_queue.put(None)
    print("End instrument production")

    """
    # it turns out to be inefficient to filter/iterate this in a dataframe
    # per https://stackoverflow.com/questions/29051573/python-filter-list-of-dictionaries-based-on-key-value
    df = pd.DataFrame.from_dict(ticker_list)
    df.cik = df['cik'].astype(str)
    # df = df[df['ticker'].isin(symbols)]  # ['TSLA', 'APPL', 'GOOG', 'GOOGL', 'DVN', 'MPC', 'SLB', 'EQT']

    for index, row in df.iteritems():
        print(f"Producer is adding instrument for {row.ticker} to the queue")
        # queue.put(generic_equity(row.ticker, row.primary_exchange, row.currency_name, row.cik))

        # print(f"Producer is yielding instrument for {row.ticker}")
        # yield generic_equity(row.ticker, row.primary_exchange, row.currency_name, row.cik)
    """


def unix_convert(ts):
    if len(str(ts)) == 13:
        ts = int(ts/1000)
        tdate = datetime.utcfromtimestamp(ts)  # .strftime('%Y-%m-%d %H:%M:%S')
        return tdate
    if len(str(ts)) == 19:
        ts = int(ts/1000000000)
        tdate = datetime.utcfromtimestamp(ts)  # .strftime('%Y-%m-%d %H:%M:%S')
        return tdate


async def get_bar_data(stocks_client, ticker: str, start_date, end_date):
    # Make API Call
    print(f"{time.time()}: get_bar_data: start getting aggregate bars for {ticker}")
    resp = await stocks_client.get_aggregate_bars(symbol=ticker
                                                  , from_date=start_date, to_date=end_date, timespan='minute'
                                                  , full_range=True, run_parallel=True, max_concurrent_workers=8
                                                  , high_volatility=True, warnings=False, adjusted=True)
    print(f"{time.time()}: get_bar_data: done getting aggregate bars for {ticker}")

    for d in resp:
        d.setdefault('a')
        d.setdefault('op')
        d.setdefault('n')
        d.setdefault('vw')
        if 'a' in d:
            del d['a']
        if 'n' in d:
            del d['n']
        if 'op' in d:
            del d['op']
    resp = [{'v': d['v'], 'vw': d['vw'], 'o': d['o'], 'c': d['c'], 'h': d['h'], 'l': d['l'], 't': d['t']} for d in resp]
    for d in resp:
        d['bar_volume'] = d.pop('v')
        d['bar_vwap'] = d.pop('vw')
        d['bar_open'] = d.pop('o')
        d['bar_close'] = d.pop('c')
        d['bar_high'] = d.pop('h')
        d['bar_low'] = d.pop('l')
        d['bar_time_start'] = d.pop('t')
        d['symbol'] = ticker
        d['bar_volume'] = int(d['bar_volume'])
        d['timestamp'] = unix_convert(d['bar_time_start'])
        d['save_date'] = datetime.now(timezone.utc)

    resp = [{'symbol': d['symbol'], 'bar_time_start': d['bar_time_start'], 'open': d['bar_open'], 'high': d['bar_high'],
             'low': d['bar_low'], 'close': d['bar_close'], 'volume': d['bar_volume'], 'vwap': d['bar_vwap'],
             'timestamp': d['timestamp'], 'save_date': d['save_date']
             } for d in resp]
    print(f"{time.time()}: get_bar_data: done formatting aggregate bars dictionary response for {ticker}")

    return resp


def consume_instrument(instrument_queue, data_queue, start_date, end_date, key):
    print("Begin instrument consumption")
    # consume items
    while True:
        # get a unit of work
        instrument = instrument_queue.get()
        # check for stop
        if instrument is None:
            break

        ticker_dict = get_bar_data(instrument.symbol, start_date, end_date, key)
        print(f'instrument consumer got data for {instrument.symbol}')

        # print(f"Producer is adding data for {instrument.symbol} to the queue")
        # data_queue.put(ticker_dict)

    # all done
    print('End instrument consumption')


async def main_async():
    key = config.polygon_key
    stocks_client = StocksClient(key, use_async=True, read_timeout=60)

    # start_date = date(2003, 1, 1)
    start_date = date(2021, 1, 1)
    end_date = date(2021, 1, 31)

    ticker_list = ticker_factory(key)

    try:
        for t in ticker_list:
            print(f"creating instrument for {t['ticker']}")
            instrument = create_equity_instrument(t['ticker'], nti.Venue(t['primary_exchange']), USD, t['cik'])

            print(f'getting data for {instrument.symbol}')
            ticker_dict = await get_bar_data(stocks_client, instrument.symbol, start_date, end_date)
    finally:
        await stocks_client.close()

if __name__ == '__main__':
    print('START - program initiated')
    asyncio.run(main_async())
    print('FINISH - program complete')

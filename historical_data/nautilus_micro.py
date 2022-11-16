import traceback
# from csv import writer
from csv import DictWriter
import pandas as pd
# from sqlalchemy import create_engine, text
from datetime import datetime, date, timedelta
import time
import config
from datetime import timezone
from polygon import StocksClient
from polygon import ReferenceClient
from typing import Iterator, Optional
import io
import asyncio
# import asyncpg
import json
from itertools import dropwhile
# import uvloop  # Unix only
# import nautilus_trader
# from nautilus_trader.model.data.tick import QuoteTick
# from nautilus_trader.model.objects import Price, Quantity
# from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.backtest.data.providers import TestInstrumentProvider
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.currency import Currency
from nautilus_trader.model.enums import AssetClass, AggregationSource, BarAggregation, PriceType
from nautilus_trader.model.enums import OptionKind
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments.currency_pair import CurrencyPair
from nautilus_trader.model.instruments.equity import Equity
from nautilus_trader.model.instruments.future import Future
from nautilus_trader.model.instruments.option import Option
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.persistence.external.core import write_objects
from nautilus_trader.model.data.bar import BarType, BarSpecification
from nautilus_trader.backtest.data.wranglers import BarDataWrangler
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from queue import Queue

WORKER_COUNT = 1

stock_cols = ['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open', 'tick_close', 'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date']
# engine = create_engine(config.psql)
key = config.polygon_key
stocks_client = StocksClient(key, use_async=True, read_timeout=60)  # Async client
symbols = ['TSLA', 'AMZN']  # ['TSLA', 'APPL', 'GOOG', 'GOOGL', 'DVN', 'MPC', 'SLB', 'EQT']

# Restart at a specific ticker
# elem = 'EEV'
# symbols = list(dropwhile(lambda x: x != elem, symbols))
# print(symbols)


def unix_convert(ts):
    if len(str(ts)) == 13:
        ts = int(ts/1000)
        tdate = datetime.utcfromtimestamp(ts)  # .strftime('%Y-%m-%d %H:%M:%S')
        return tdate
    if len(str(ts)) == 19:
        ts = int(ts/1000000000)
        tdate = datetime.utcfromtimestamp(ts)  # .strftime('%Y-%m-%d %H:%M:%S')
        return tdate


async def get_bar_data(ticker: str, start_date, end_date):
    # Make API Call
    # Note that while this is calling the routine with await, it is also running parallel threads to pull down
    # data on lots of network connections. It's unclear the extent to which this will actually yield to other
    # coroutines during its work. It may be that this operation alone takes all the resources of the machine
    # making "await" functionally useless

    # start = time.time()
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
        d['timestamp'] = unix_convert(d['bar_time_start'])  # pd.Timestamp(d['bar_time_start'], tz='UTC')  # pd.Timestamp(datetime.datetime.strptime(d['bar_time_start'].decode(), "%Y%m%d %H%M%S%f"), tz='UTC')
        d['save_date'] = datetime.now(timezone.utc)

    resp = [{'symbol': d['symbol'], 'bar_time_start': d['bar_time_start'], 'open': d['bar_open'], 'high': d['bar_high'],
             'low': d['bar_low'], 'close': d['bar_close'], 'volume': d['bar_volume'], 'vwap': d['bar_vwap'],
             'timestamp': d['timestamp'], 'save_date': d['save_date']
             } for d in resp]
    print(f"{time.time()}: get_bar_data: done formatting aggregate bars dictionary response for {ticker}")

    return resp


async def get_instrument_data(instrument):
    print(f"Getting data for {instrument.symbol}")

    # start_date = date(2003, 1, 1)
    start_date = date(2021, 1, 1)
    end_date = date(2021, 1, 31)

    try:
        # Get Data for a Group of Tickers
        t = time.time()
        t3 = time.time()

        # gets a list of dictionaries, each dictionary is a tick for a single equity
        ticker_dict = await get_bar_data(instrument.symbol, start_date, end_date)
        print("Time to data:", time.time()-t3)

        t4 = time.time()
        df = pd.DataFrame(ticker_dict)
        df.set_index('timestamp', inplace=True)
        print("Time to dataframe:", time.time()-t4)

        instrument_id = InstrumentId(instrument.symbol, instrument.venue)

        bar_type = BarType(
            instrument_id,
            BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
            AggregationSource.EXTERNAL,  # Bars are aggregated in the data already
        )

        wrangler = BarDataWrangler(bar_type, instrument)
        print(f"Done a-wranglin for {instrument.symbol}!")
        bars = wrangler.process(data=df[['open', 'high', 'low', 'close', 'volume']])
        return bars

    except Exception as e:
        print(e)
        traceback.print_exc()
        return


def generic_equity(ticker_symbol: str, ticker_venue: Venue, currency, cik) -> Equity:
    return Equity(
        instrument_id=InstrumentId(symbol=Symbol(ticker_symbol), venue=Venue(ticker_venue)),
        native_symbol=Symbol(ticker_symbol),
        currency=USD,
        price_precision=2,
        price_increment=Price.from_str("0.01"),
        multiplier=Quantity.from_int(1),
        lot_size=Quantity.from_int(1),
        isin=cik,  # we should be converting this to isin...
        ts_event=0,
        ts_init=0,
    )


async def heartbeat():
    while True:
        start = time.time()
        await asyncio.sleep(1)
        delay = time.time() - start - 1
        print(f'heartbeat delay = {delay:.3f}s')


async def instrument_producer_async(queue, symbol_list):
    ref_client = ReferenceClient(key, use_async=True)  # for an async client
    tickers = await ref_client.get_tickers(market='stocks', limit=1, all_pages=False, merge_all_pages=False, max_pages=1)

    for ticker in tickers:
        df = pd.DataFrame.from_dict(ticker)
        df.cik = df['cik'].astype(str)
        df = df[df['ticker'].isin(symbol_list)]

        for index, row in df.iterrows():
            print(f"Producer is adding instrument for {row.ticker} to the queue")
            queue.put(generic_equity(row.ticker, row.primary_exchange, row.currency_name, row.cik))
            # print(f"Producer is yielding instrument for {row.ticker}")
            # yield generic_equity(row.ticker, row.primary_exchange, row.currency_name, row.cik)

    print("Done producing")
    pass


def instrument_producer(queue, symbol_list):
    ref_client = ReferenceClient(key, use_async=False)  # for an async client
    tickers = ref_client.get_tickers(market='stocks', all_pages=True, merge_all_pages=True)
    df = pd.DataFrame.from_dict(tickers)
    df.cik = df['cik'].astype(str)
    df = df[df['ticker'].isin(symbol_list)]

    for index, row in df.iterrows():
        print(f"Producer is adding instrument for {row.ticker} to the queue")
        queue.put(generic_equity(row.ticker, row.primary_exchange, row.currency_name, row.cik))
        # print(f"Producer is yielding instrument for {row.ticker}")
        # yield generic_equity(row.ticker, row.primary_exchange, row.currency_name, row.cik)

    print("Done producing")


async def consume_instrument_async(queue):
    catalog_path = 'C:\Repos\polygon_nautilus\.data\polygon_nautilus'
    catalog = ParquetDataCatalog(catalog_path)
    # print(f"Writing instrument {instrument.symbol} to catalog ")
    # write_objects(catalog, [instrument])

    while True:
        instrument = await queue.get()
        print(f"Writing instrument {instrument.symbol} to catalog ")
        write_objects(catalog, [instrument])
        queue.task_done()

    print("Async done consuming")


def consume_instrument(queue):
    catalog_path = 'C:\Repos\polygon_nautilus\.data\polygon_nautilus'
    catalog = ParquetDataCatalog(catalog_path)
    # print(f"Writing instrument {instrument.symbol} to catalog ")
    # write_objects(catalog, [instrument])

    while not queue.empty():
        instrument = queue.get()
        print(f"Writing instrument {instrument.symbol} to catalog ")
        write_objects(catalog, [instrument])

    print("Done consuming")


async def main_async():
    asyncio.create_task(heartbeat())
    await asyncio.sleep(2.5)

    q = asyncio.Queue(maxsize=2)
    futures = []

    with ThreadPoolExecutor(max_workers=4) as executor:
        producer_future = executor.submit(instrument_producer, q, symbols)
        futures.append(producer_future)

        workers = [asyncio.create_task(consume_instrument(q)) for _ in range(3)]

        for _ in range(3):
            consumer_future = executor.submit(consume_instrument, q)
            futures.append(consumer_future)

        await queue.join()

        for future in as_completed(futures):
            future.result()

    await asyncio.sleep(5)

    # nautilus_bars = asyncio.run(get_instrument_data(instrument))
    # write bars to catalog

    print(f"Done processing all the tickers")

    """
    try:

    except Exception as e:
        print(e)
        traceback.print_exc()

    finally:
        await stocks_client.close()
    """


async def main():


    with ThreadPoolExecutor(max_workers=4) as executor:
        producer_future = executor.submit(instrument_producer, q, symbols)
        futures.append(producer_future)

        for _ in range(3):
            consumer_future = executor.submit(consume_instrument, q)
            futures.append(consumer_future)

        for future in as_completed(futures):
            future.result()

    await asyncio.sleep(5)

    # nautilus_bars = asyncio.run(get_instrument_data(instrument))
    # write bars to catalog

    print(f"Done processing all the tickers")


if __name__ == '__main__':
    print('in main')
    asyncio.run(main_async())
    print('All the way done')


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

key = config.polygon_key


# stock_cols = ['symbol', 'tick_volume', 'total_volume', 'opening_price', 'tick_vwap', 'tick_open', 'tick_close', 'tick_high', 'tick_low', 'vwap', 'avg_trade_size', 'time_beg', 'time_end', 'tdate', 'save_date']
# engine = create_engine(config.psql)
# stocks_client = StocksClient(key, use_async=True, read_timeout=60)     # Async client
# symbols = ['TSLA', 'AMZN']

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


"""
async def sql_fun(data, conn):
    sio = [tuple(d.values()) for d in data]
    await conn.copy_records_to_table('stockdata_hist', records=sio, columns=stock_cols)
    del sio
"""

async def get_bar_data(stocks_client, ticker: str, start_date, end_date):
    # Make API Call
    resp = await stocks_client.get_aggregate_bars(symbol=ticker
                                                  , from_date=start_date, to_date=end_date, timespan='minute'
                                                  , full_range=True, run_parallel=True, max_concurrent_workers=8
                                                  , high_volatility=True, warnings=False, adjusted=True)
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

    return resp


async def old_main(instrument):
    print(f"Getting data for {len(symbols)} symbols")
    counter = 0
    times_list = []

    list_len = 20
    symbols_list = [symbols[x:x + list_len] for x in range(0, len(symbols), list_len)]
    # start_date = date(2003, 1, 1)
    start_date = date(2021, 1, 1)
    end_date = date(2021, 1, 31)
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
                        print(f"Getting data for {ticker}")
                        t3 = time.time()

                        # gets a list of dictionaries, each dictionary is a tick for a single equity
                        ticker_dict = await get_bar_data(ticker, start_date, end_date1)
                        print("Time to data:", time.time()-t3)

                        t4 = time.time()
                        df = pd.DataFrame(ticker_dict)
                        print("Time to dataframe:", time.time()-t4)

                        # Chris Sellers mentioned Redis only had int64 decimal resolution of "something", which was insufficient
                        # dataframe has date in tdate, and "something" in time_end (nanoseconds?) 1609750800000
                        # dt = pd.Timestamp(datetime.datetime.strptime(data['timestamp'].decode(), "%Y%m%d %H%M%S%f"), tz='UTC')

                        df.set_index('timestamp', inplace=True)
                        # df.tz_localize('UTC', level=0)
                        # df.index = pd.to_datetime(df.pop('timestamp'), utc=True)
                        #dt = pd.Timestamp(data["timestamp"], tz="UTC")
                        print("index done")

                        instrument_id = InstrumentId(instrument.symbol, instrument.venue)

                        bar_type = BarType(
                            instrument_id,
                            BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
                            AggregationSource.EXTERNAL,  # Bars are aggregated in the data already
                        )

                        wrangler = BarDataWrangler(bar_type, instrument)
                        ticks = wrangler.process(data=df[['open', 'high', 'low', 'close', 'volume']])
                        print('Done a-wranglin!')

                        #dt = pd.Timestamp(data["timestamp"], tz="UTC")
                        #print(dt)



                        # write to data catalog
                        # with open(f'C:\Repos\polygon_nautilus\.data\{ticker}.csv', 'a') as f_object:
                        #     dictwriter_object = DictWriter(f_object, fieldnames=field_names)
                        #     for d in new:
                        #         dictwriter_object.writerow(d)
                        #     f_object.close()


                        # df = df + new
                        times_list.append((time.time() - t))
                    except Exception as e:
                        print(e)
                        traceback.print_exc()
                        pass

                t2 = time.time()

                # sio was meant to organize the data for storage to a postgres database
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

                print("sql time: ", time.time()-t2)
                print("Average: ", (sum(times_list) / len(times_list)))  # Time it
            print('Updating dates')
            print(start_date)
            start_date = end_date1 + timedelta(days=1)
            end_date1 = end_date
            print(start_date, end_date1)

        except Exception as e:
            print(e)
            traceback.print_exc()
            pass

    await stocks_client.close()

    return


def generic_equity(ticker_symbol: str, ticker_venue: str, currency, cik) -> Equity:
    equity = Equity(
        instrument_id=InstrumentId(Symbol(ticker_symbol), Venue(ticker_venue)),
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
    return equity


def ticker_factory():
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


async def main():
    # catalog_path = 'C:\Repos\polygon_nautilus\.data\polygon_nautilus'

    # Create an instance of the ParquetDataCatalog
    # catalog = ParquetDataCatalog(catalog_path)

    # start_date = date(2003, 1, 1)
    start_date = date(2021, 1, 1)
    end_date = date(2021, 1, 31)

    ticker_list = ticker_factory(key)

    stocks_client = StocksClient(key, use_async=True, read_timeout=60)

    # for index, row in df.iterrows():
    #    instrument = generic_equity(row.ticker, row.primary_exchange, row.currency_name, row.cik)

    try:
        for t in ticker_list:
            print(f"creating instrument for {t['ticker']}")
            instrument = generic_equity(t['ticker'], t['primary_exchange'], USD, t['cik'])

            print(f'getting data for {instrument.symbol}')
            ticker_dict = await get_bar_data(stocks_client, instrument.symbol, start_date, end_date)
    finally:
        await stocks_client.close()


if __name__ == '__main__':
    print('program start')
    asyncio.run(main())
    print('program end')

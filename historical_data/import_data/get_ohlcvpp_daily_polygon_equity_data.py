import traceback
import pandas as pd
from datetime import datetime, date, timedelta
import time
import config
from datetime import timezone
import asyncio
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data.bar import BarType, BarSpecification
from nautilus_trader.model.enums import AssetClass, AggregationSource, BarAggregation, PriceType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments.equity import Equity
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.external.core import write_objects

# import uvloop  # Unix only
from polygon.stocks import StocksClient
from polygon import ReferenceClient

key = config.polygon_key

"""
This is what we have used to create 1 minute bar Nautilus catalogs
"""


def unix_convert(ts):
    if len(str(ts)) == 13:
        ts = int(ts / 1000)
    elif len(str(ts)) == 19:
        ts = int(ts / 1000000000)
    else:
        raise TypeError(f'timestamp length {len(str(ts))} was not 13 or 19')

    return datetime.utcfromtimestamp(ts)  # .strftime('%Y-%m-%d %H:%M:%S')


async def get_ohlcvpp_data(stocks_client, ticker: str, trading_date):
    resp = await stocks_client.get_daily_open_close(symbol=ticker, date=trading_date, adjusted=True)

    # For each day, choose the component you want to investigate further, like "close"
    if resp['close']
        # go hunting for the data that represents the closing price event for the day


    if resp['status'] == 'OK':
        return resp  # afterHours, close, from, high, low, open, preMarket, status, symbol, volume
    else:
        return False

def generic_equity(ticker_symbol: str, ticker_venue: str, currency, cik) -> Equity:
    return Equity(
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


def get_polygon_key():
    return config.polygon_key


def ticker_factory():
    print("Retrieving tickers")
    ref_client = ReferenceClient(get_polygon_key(), use_async=False)

    try:
        tickers = ref_client.get_tickers(market='stocks', all_pages=True, merge_all_pages=True)
        # filter here if you are just testing, or need to skip symbols
        # tickers = [d for d in tickers if d['ticker'] in symbols]
        tickers = [d for d in tickers if d['ticker'] == 'QQQ']
        print("Tickers retrieved")
        return tickers
    finally:
        ref_client.close()


async def main():
    stocks_client = StocksClient(api_key=get_polygon_key(), use_async=True, read_timeout=60)

    try:
        catalog_path = "C:/Data/Polygon/daily_open_close"
        catalog = ParquetDataCatalog(catalog_path)
        start_date = date(2015, 5, 1)
        end_date = date(2023, 5, 27)

        ticker_list = ticker_factory()
        for ticker in ticker_list:
            t = time.time()
            ticker_dict = {}

            print(f"Getting data for {ticker}")

            current_date = start_date
            while current_date <= end_date:
                try:
                    data = await get_ohlcvpp_data(stocks_client, ticker['ticker'], current_date)
                    if data:
                        ticker_dict[current_date] = data
                        # print(f"Data added for {current_date}")
                    elif current_date.isoweekday() < 6:
                        print(f"No data for {current_date}")

                except Exception as e:
                    # loop forever, reconnecting and retrying
                    print(e)
                    traceback.print_exc()
                    print(f"ERROR:: Bugger! Gonna sleep for a tick, then retry ticker {ticker['ticker']}")

                    try:
                        await asyncio.sleep(3)
                        await stocks_client.close()
                    except Exception:
                        pass

                    stocks_client = StocksClient(api_key=get_polygon_key(), use_async=True, read_timeout=60)
                else:
                    current_date += timedelta(days=1)  # Advance to next date

            if ticker_dict:
                print("Time to data:", time.time() - t)
                # t = time.time()

                # afterHours, close, from, high, low, open, preMarket, status, symbol, volume


                df = pd.DataFrame(ticker_dict)
                df.set_index('timestamp', inplace=True)
                # print("Time to dataframe:", time.time() - t)

                # t = time.time()
                ticker.setdefault("cik", "") # create an attribute and set it to nothing, to conform to the Naut spec

                try:
                    instrument = generic_equity(ticker['ticker'], ticker['primary_exchange'], USD, str(ticker['cik']))
                except Exception as e:
                    print(e)
                    traceback.print_exc()
                    print(f"ERROR:: Bugger, couldn't create an instrument. Ah well, sally forth!")
                    continue
                    continue

                try:
                    bar_type = BarType(
                        instrument.id,
                        BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
                        AggregationSource.EXTERNAL,  # Bars are aggregated in the data already
                    )
                    wrangler = BarDataWrangler(bar_type, instrument)
                    ticks = wrangler.process(data=df[['open', 'high', 'low', 'close', 'volume']], ts_init_delta=1)
                    # print("Time to wrangle:", time.time() - t)

                    # t = time.time()
                    write_objects(catalog, [instrument])
                    write_objects(catalog, ticks)
                    print("Time to persist:", time.time() - t)
                except Exception as e:
                    print(e)
                    traceback.print_exc()
                    print(
                        f"ERROR:: Bugger, couldn't create bar_type, wrangler, ticks, or commit. Ah well, sally forth!")
                    continue
            else:
                print(f"No data for {ticker['ticker']}, carrying on")

    except Exception as e:
        print(e)
        traceback.print_exc()

    finally:
        await stocks_client.close()

    return


if __name__ == '__main__':
    # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # Unix only
    print('program start')
    asyncio.run(main())
    print('program end')

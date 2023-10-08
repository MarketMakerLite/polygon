"""this file conatins a class PolygonEquityData 
to read data from polygon. We can use this 
class across the repo to pull data from
polygon.
"""
import logging
import pandas as pd
from datetime import datetime, timezone
from polygon import StocksClient
from polygon import ReferenceClient
from nautilus_trader.model.currencies import USD
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data.bar import BarType, BarSpecification
from nautilus_trader.model.enums import (
    AggregationSource,
    BarAggregation,
    PriceType,
)
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments.equity import Equity
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.external.core import write_objects


class PolygonEquityData:
    """
    Class to read data from Polygon API
    Usage:
        poly = PolygonEquityData(config, ["QQQ"], "/workspaces/data")
        poly.get_tickers()
        poly.get_bar_data_for_tickers(date(2022, 1, 1), date(2023, 3, 30))

        bar_df = poly.read_catalog_data()
    """

    def __init__(self, config, ticker_list, catalog_path):
        """
        Args:
            config (module): config containing polygon api key
            ticker_list (list): list of string tickers
            catalog_path (str): path to create nautilus catalog
        """
        self.config = config
        self.ticker_list = ticker_list
        self.catalog_path = catalog_path

    def get_tickers(self):
        """pull ticker list from polygon and filter for required
        tickers

        Returns:
            None
        """
        logging.info("Retrieving tickers")
        ref_client = ReferenceClient(self.config.polygon_key, use_async=False)

        try:
            tickers = ref_client.get_tickers(
                market="stocks", all_pages=True, merge_all_pages=True
            )
            self.tickers = [d for d in tickers if d["ticker"] in self.ticker_list]
            logging.info("Tickers retrieved")
        finally:
            ref_client.close()

    def get_bar_data_for_tickers(self, start_date, end_date):
        """loop through tickers and call
        get_bar_data_for_single_ticker for each ticker. Also
        save the data in nautilus catalog

        Args:
            start_date (date):
            end_date (date):
        """
        stocks_client = StocksClient(
            self.config.polygon_key, use_async=False, read_timeout=120
        )
        logging.info(f"creating nautilus catalog at {self.catalog_path}")
        catalog = ParquetDataCatalog(self.catalog_path)
        for ticker in self.tickers:
            logging.info(f"running polygon api for ticker {ticker['ticker']}")
            ticker_data = self.get_bar_data_for_single_ticker(
                stocks_client, ticker["ticker"], start_date, end_date
            )
            instrument = self.create_nautilus_equity(
                ticker["ticker"], ticker["primary_exchange"], str(ticker["cik"]), USD
            )
            write_objects(catalog, [instrument])
            df = pd.DataFrame(ticker_data)
            df.set_index("timestamp", inplace=True)
            bar_type = BarType(
                instrument.id,
                BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
                AggregationSource.EXTERNAL,
            )
            wrangler = BarDataWrangler(bar_type, instrument)
            bars = wrangler.process(
                data=df[["open", "high", "low", "close", "volume"]], ts_init_delta=1
            )
            logging.info(f"saving {ticker['ticker']} data to catalog")
            write_objects(catalog, bars)
        stocks_client.close()

    def get_bar_data_for_single_ticker(
        self, stocks_client, ticker, start_date, end_date
    ):
        """get bar data for a single ticker

        Args:
            stocks_client (StocksClient): polygon api
            ticker (str):
            start_date (date):
            end_date (date):

        Returns:
            list: list of dicts containing bar data
        """
        resp = stocks_client.get_aggregate_bars(
            symbol=ticker,
            from_date=start_date,
            to_date=end_date,
            timespan="minute",
            full_range=True,
            run_parallel=True,
            high_volatility=True,
            warnings=False,
            adjusted=True,
        )

        for d in resp:
            d.setdefault("a")
            d.setdefault("op")
            d.setdefault("n")
            d.setdefault("vw")
            if "a" in d:
                del d["a"]
            if "n" in d:
                del d["n"]
            if "op" in d:
                del d["op"]
        resp = [
            {
                "v": d["v"],
                "vw": d["vw"],
                "o": d["o"],
                "c": d["c"],
                "h": d["h"],
                "l": d["l"],
                "t": d["t"],
            }
            for d in resp
        ]
        for d in resp:
            d["bar_volume"] = d.pop("v")
            d["bar_vwap"] = d.pop("vw")
            d["bar_open"] = d.pop("o")
            d["bar_close"] = d.pop("c")
            d["bar_high"] = d.pop("h")
            d["bar_low"] = d.pop("l")
            d["bar_time_start"] = d.pop("t")
            d["symbol"] = ticker
            d["bar_volume"] = int(d["bar_volume"])
            d["timestamp"] = PolygonEquityData.convert_unix_to_datetime(
                d["bar_time_start"] * 1000000 + 59999999999
            )  # add 1 minute -1 nanoseconds
            d["save_date"] = datetime.now(timezone.utc)

        resp = [
            {
                "symbol": d["symbol"],
                "bar_time_start": d["bar_time_start"],
                "open": d["bar_open"],
                "high": d["bar_high"],
                "low": d["bar_low"],
                "close": d["bar_close"],
                "volume": d["bar_volume"],
                "vwap": d["bar_vwap"],
                "timestamp": d["timestamp"],
                "save_date": d["save_date"],
            }
            for d in resp
        ]
        return resp

    def create_nautilus_equity(
        self,
        ticker_symbol,
        ticker_venue,
        cik,
        currency=USD,
    ):
        """create a nautilus instrument

        Args:
            ticker_symbol (str): name of the symbol
            ticker_venue (str): name of the venue
            cik (str): isin
            currency (nautilus_trader.model.currency.Currency, optional): nautilus currency. Defaults to USD.

        Returns:
            Equity: nautilus equity instrument
        """
        return Equity(
            instrument_id=InstrumentId(Symbol(ticker_symbol), Venue(ticker_venue)),
            raw_symbol=Symbol(ticker_symbol),
            currency=currency,
            price_precision=2,
            price_increment=Price.from_str("0.01"),
            multiplier=Quantity.from_int(1),
            lot_size=Quantity.from_int(1),
            isin=cik,
            ts_event=0,
            ts_init=0,
        )

    @staticmethod
    def convert_unix_to_datetime(ts):
        """convert from unix time to python datetime

        Args:
            ts (int): unix time

        Raises:
            TypeError: in case unix timestamp is invalid

        Returns:
            date: return python date
        """
        if len(str(ts)) == 13:
            ts = int(ts / 1000)
        elif len(str(ts)) == 19:
            ts = int(ts / 1000000000)
        else:
            raise TypeError(f"timestamp length {len(str(ts))} was not 13 or 19")
        return datetime.utcfromtimestamp(ts)

    def read_catalog_data(self, start=None, end=None):
        """read catalog data and return a dataframe

        Args:
            start (date, optional): Defaults to None.
            end (date, optional): Defaults to None.

        Returns:
            dataframe: pandas dataframe containing bar data
        """
        logging.info(f"reading data from {self.catalog_path}")
        catalog = ParquetDataCatalog(self.catalog_path)
        bars = catalog.bars(start=start, end=end)
        return bars

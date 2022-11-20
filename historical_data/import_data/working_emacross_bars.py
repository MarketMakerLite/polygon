import datetime
import os
import shutil
from decimal import Decimal

import fsspec
import pandas as pd
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data.bar import Bar  #QuoteTick
from nautilus_trader.model.objects import Price, Quantity

from nautilus_trader.backtest.data.providers import TestInstrumentProvider
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.external.core import process_files, write_objects
from nautilus_trader.persistence.external.readers import TextReader

# RKM i had to import these..
from nautilus_trader.config.backtest import BacktestVenueConfig, BacktestDataConfig, BacktestRunConfig, BacktestEngineConfig
from nautilus_trader.config.common import ImportableStrategyConfig
from nautilus_trader.examples.strategies.ema_cross import EMACrossConfig

if __name__ == "__main__":
    CATALOG_PATH = "C:\\Polygon\\Nautilus_1minbars"  # \\data"  # os.getcwd() + "/catalog" # "\\catalog" looks better but isn't necessary
    # Load existing catalog (only if not already created above)
    catalog = ParquetDataCatalog(CATALOG_PATH)

    df = catalog.instruments()

    instrument = catalog.instruments(as_nautilus=True)[0]

    start = dt_to_unix_nanos(pd.Timestamp('2022-01-01', tz='UTC'))
    end = dt_to_unix_nanos(pd.Timestamp('2022-02-01', tz='UTC'))

    #catalog.quote_ticks(start=start, end=end)

    # df = catalog.bars(instrument_ids=['AMZN.XNAS', 'TSLA.XNAS', 'GOOGL.XNAS', 'IBM.XNAS', 'XLY.XNAS'], start=start, end=end)

    #instrument = catalog.instruments(as_nautilus=True)[0]

    venues_config = [
        BacktestVenueConfig(
            name="XNAS",
            oms_type="HEDGING",
            account_type="MARGIN",
            base_currency="USD",
            starting_balances=["1000000 USD"],
        )
    ]

    data_config = [
        BacktestDataConfig(
            catalog_path=str(catalog.path),
            # CATALOG_PATH, # , #"C:\\Repos\\Lab\\GettingStarted\\demo\\demo\\catalog", #data\\quote_tick.parquet", #str(ParquetDataCatalog.from_env().path),
            data_cls=Bar,  #QuoteTick,
            instrument_id=instrument.id.value,  # "EUR-USD.SIM",
            start_time=start,  # 1580398089820000000,
            end_time=end,  # 1580504394501000000,
        )
    ]

    strategies = [
        ImportableStrategyConfig(
            strategy_path="nautilus_trader.examples.strategies.ema_cross:EMACross",
            config_path="nautilus_trader.examples.strategies.ema_cross:EMACrossConfig",
            config=EMACrossConfig(
                instrument_id=instrument.id.value,  # "EUR-USD.SIM",
                bar_type="AMZN.XNAS-1-MINUTE-LAST-EXTERNAL",  # "EUR/USD.SIM-15-MINUTE-BID-INTERNAL",
                fast_ema=10,  # 10,
                slow_ema=20,  # 20,
                trade_size=Decimal(1_000),  # Decimal(1_000_000),
            ),
        ),
    ]

    config = BacktestRunConfig(
        engine=BacktestEngineConfig(strategies=strategies),
        data=data_config,
        venues=venues_config,
    )

    node = BacktestNode(configs=[config])

    results = node.run()

    print("hello")

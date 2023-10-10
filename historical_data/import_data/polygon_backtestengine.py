from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.persistence.catalog import ParquetDataCatalog


config = BacktestEngineConfig(
    trader_id="BACKTESTER-1"
)


engine = BacktestEngine(config)


catalog = ParquetDataCatalog("/workspaces/data")
instruments = catalog.instruments(as_nautilus=True)
engine.add_venue('XNAS')
for instrument in instruments:
    engine.add_instrument(instrument)


engine.run()

# from nautilus_trader.model.enums import AccountType
# from nautilus_trader.model.enums import OmsType
# from nautilus_trader.model.identifiers import Venue
# from nautilus_trader.config import BacktestVenueConfig

# venue = Venue("SIM")

# venue_config = BacktestVenueConfig(
#     name=venue.value,
#     oms_type=OmsType.HEDGING,
#     account_type=AccountType.MARGIN,
#     base_currency="USD",
#     starting_balances=["100_000 USD"],
# )

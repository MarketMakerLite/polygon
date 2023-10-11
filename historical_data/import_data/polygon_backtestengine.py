import pathlib
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.model.data import BarType
from nautilus_trader.model.currencies import USD
from historical_data.import_data.parse_markethours import parse_markethours
from historical_data.import_data.simple_strategy import PrintDataStrategy

config = BacktestEngineConfig(trader_id="BACKTESTER-1")

engine = BacktestEngine(config)

catalog = ParquetDataCatalog("/workspaces/data")
instruments = catalog.instruments(as_nautilus=True)
status = []
engine.add_venue(
    venue=Venue("XNAS"),
    oms_type=OmsType.HEDGING,
    account_type=AccountType.MARGIN,
    starting_balances=[Money(1_000_000, USD)],
)
for instrument in instruments:
    engine.add_instrument(instrument)
    status = status + parse_markethours(
        pathlib.Path(__file__).parent.joinpath("MarketHours.csv"), instrument
    )

engine.add_data(catalog.bars(as_nautilus=True))
engine.add_data(status)
engine.add_strategy(
    PrintDataStrategy(
        instrument=instruments[0],
        bar_type=BarType.from_str("QQQ.XNAS-1-MINUTE-LAST-EXTERNAL"),
    )
)
engine.run()

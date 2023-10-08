import config
from datetime import date
import logging
from historical_data.import_data.polygon_equity_data import PolygonEquityData

logging.basicConfig(level=logging.INFO)
# test PolygonEquityData class
poly = PolygonEquityData(config, ["QQQ"], "/workspaces/data")
poly.get_tickers()
poly.get_bar_data_for_tickers(date(2023, 9, 16), date(2023, 9, 30))

bar_df = poly.read_catalog_data()
print(bar_df.head())

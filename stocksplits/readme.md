# Polygon Stock Splits

This script is designed to be run once a day (after market close) via CronTab or similar scheduling program in order to maintain an existing database of stock data (in this case, 1 minute aggregates).

The core design is as follows:
* Loop through all tickers to check for any ticker with a stock split that has an ex-dividend date of today
* Download new, adjusted historical data from polygon
* Delete existing data from database
* Write new data to database

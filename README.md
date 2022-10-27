# NautilusTrader Polygon Repository
[![Python Badge](https://img.shields.io/badge/made%20with-python-blue.svg)]()
[![GitHub license](https://badgen.net/github/license/MarketMakerLite/polygon)](https://github.com/MarketMakerLite/polygon/blob/master/LICENSE)
[![GitHub last commit](https://img.shields.io/github/last-commit/MarketMakerLite/polygon)](https://github.com/MarketMakerLite/polygon/commits/main)
[![Discord](https://img.shields.io/discord/837528551028817930?color=%237289DA&label=Discord)](https://discord.gg/jjDcZcqXWy)

A repository of code that interacts with the [Polygon API by pssolanki111](https://github.com/pssolanki111/polygon)

## Websockets
The websocket files send data to a Redis list, which is then consumed through the listener script - saving the data in chunks to a NautilusTrader parquet data catalog.

### crypto_stream.py
This script streams aggregated minutely bars for all crypto pairs available on Polygon. This data is fed to a Redis list to be consumed by the listener script. 

### options_stream.py
This script streams options trades for all options available on Polygon. This data is fed to a Redis list to be consumed by the listener script. 

### stock_stream.py
This script streams aggregated seconds for all stocks available on Polygon. This data is fed to a Redis list to be consumed by the listener script. 

### stream_listener.py
This script listens and consumes the Redis lists created by the streaming scripts. 

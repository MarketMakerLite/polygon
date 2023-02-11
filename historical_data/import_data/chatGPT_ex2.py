import aiohttp
import asyncio
import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time
from typing import Any, Dict, List, Tuple
import json


# Part 1: Asynchronous download of data

async def download_trade_data(symbol: str, date: str, session: aiohttp.ClientSession) -> Dict[str, Any]:
    # Replace YOUR_API_KEY with your actual API key
    API_KEY = "insertyourAPIkeyhere"

    # Set the headers for the request
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }

    """Asynchronously download trade data for the given date from Polygon.io"""
    base_url = 'https://api.polygon.io/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}'
    params = {
        'stocksTicker' : symbol,
        'multiplier' : '1', # Interval in minutes
        'timespan' : 'minute',
        'from' : date,
        'to' : date
    }
    url = base_url.format(**params)

    async with session.get(url, headers=headers) as resp:
        data = await resp.read()
        trade_data = json.loads(data)
    return trade_data


async def download_trade_data_for_symbol(symbol: str, date: str) -> Dict[str, Any]:
    """Download trade data for a given symbol and date using an asyncio event loop"""
    async with aiohttp.ClientSession() as session:
        data = await download_trade_data(symbol, date, session)
    return data


async def download_trade_data_for_symbols(symbols: List[str], date: str) -> List[Dict[str, Any]]:
    """Download trade data for a list of symbols and a given date using asyncio tasks"""
    tasks = [asyncio.create_task(download_trade_data_for_symbol(symbol, date)) for symbol in symbols]
    data = await asyncio.gather(*tasks)
    return data


# Part 2: Writing to Parquet files

def write_data_to_parquet(data: List[Dict[str, Any]], date: str) -> None:
    for dict in data:
        """Write data to a Parquet file with a filename based on the date"""
        # Convert data to Arrow Table
        # table = pa.Table.from_pydict({k: pa.array(v) for k, v in data.items()})
        df = pd.DataFrame.from_dict(dict)

        # Convert the DataFrame to a Parquet file
        table = pa.Table.from_pandas(df)

        # Set filename based on date
        filename = f"trade_data_{date}.parquet"

        # Write Arrow Table to Parquet file
        pq.write_table(table, filename)


# Part 3: Main function to run everything

def main(symbols: List[str], start_date: str, end_date: str) -> None:
    """Main function to download and write trade data for a list of symbols and a given date range"""
    # Convert start and end dates to date objects
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    # Loop through each day in the date range
    for i in range((end_date - start_date).days + 1):
        date = start_date + datetime.timedelta(days=i)

        # Download trade data
        data = asyncio.run(download_trade_data_for_symbols(symbols, date))

        # Write data to Parquet file
        write_data_to_parquet(data, date)


if __name__ == "__main__":
    # List of symbols to download trade data for
    symbols = ["AAPL", "GOOG", "MSFT"]

    # Start and end dates for date range
    start_date = "2022-01-04"
    end_date = "2022-01-07"

    main(symbols, start_date, end_date)


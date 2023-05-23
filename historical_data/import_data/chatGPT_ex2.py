import aiohttp
import asyncio
import concurrent.futures
import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import time
from typing import Any, Dict, List, Tuple
import json
import config

API_KEY = config.polygon_key

"""" TO DO
1 file per equity, per year
"""

# Part 1: Asynchronous download of data

async def download_trade_data(symbol: str, date: str, session: aiohttp.ClientSession) -> Dict[str, Any]:
    # Set the headers for the request
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }

    # base_url = 'https://api.polygon.io/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}'
    base_url = 'https://api.polygon.io/v1/open-close/AAPL/2023-01-09?adjusted=true&apiKey=te2_SeJD3ud9qn7v3c7fXCTJ0tqAhapt'
    # base_url = 'https://api.polygon.io/v1/open-close/{stocksTicker}/{date}'
    params = {
        'stocksTicker': symbol,
        'multiplier': '1',  # Interval in minutes
        'timespan': 'minute',
        'from': date,
        'to': date
    }
    url = base_url.format(**params)

    async with session.get(url, headers=headers) as resp:
        data = await resp.read()
        trade_data = json.loads(data)
    return trade_data


"""
async def old__download_daily_open_close_data(symbol: str, date: str, adj: bool, session: aiohttp.ClientSession) -> Dict[str, Any]:
    # Set the headers for the request
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }

    base_url = 'https://api.polygon.io/v1/open-close/{stocksTicker}/{date}'
    params = {
        'stocksTicker': symbol,
        'date': date,
        'adjusted': adj
    }
    url = base_url.format(**params)

    async with session.get(url, headers=headers) as resp:
        data = await resp.read()
        trade_data = json.loads(data)
    return trade_data
"""


# Part 2: Writing to Parquet files

def write_parquet_file(output_path: str, data: pd.DataFrame) -> None:
    table = pa.Table.from_pandas(df=data)
    pq.write_table(table, output_path)


def write_data_to_parquet(data: List[Dict[str, Any]], symbol: str, folder: str) -> None:
    """Write a list of trade data dictionaries to a Parquet file"""
    df = pd.DataFrame.from_records(data)
    filename = f"{symbol}.parquet"
    output_path = os.path.join(folder, filename)

    # If the file already exists, delete it before writing the new data
    if os.path.exists(output_path):
        os.remove(output_path)

    write_parquet_file(output_path, df)


async def download_daily_open_close_data(symbol: str, date: str, adj: bool, session: aiohttp.ClientSession) -> Dict[str, Any]:
    # Set the headers for the request
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }

    # base_url = 'https://api.polygon.io/v1/open-close/AAPL/2023-01-09?adjusted=true&apiKey=te2_SeJD3ud9qn7v3c7fXCTJ0tqAhapt'
    base_url = 'https://api.polygon.io/v1/open-close/{stocksTicker}/{date}?adjusted={adj}'
    url = base_url.format(stocksTicker=symbol, date=date, adj=str(adj).lower())

    async with session.get(url, headers=headers) as resp:
        data = await resp.json()
    return data


async def download_daily_open_close_data_for_symbol(symbol: str, date: str, adj: bool) -> Dict[str, Any]:
    async with aiohttp.ClientSession() as session:
        data = await download_daily_open_close_data(symbol, date, adj, session)
    return data


async def download_data_for_symbol(symbol: str, start_date: datetime, end_date: datetime, adj: bool):
    """Download trade data for a single symbol and a given date range"""
    symbol_data = []
    for i in range((end_date - start_date).days + 1):
        date = (start_date + datetime.timedelta(days=i)).strftime('%Y-%m-%d')
        data = await download_daily_open_close_data_for_symbol(symbol, date, adj)
        symbol_data.extend(data)
    return symbol_data


async def download_data_for_symbols(symbol_list: List[str], start_date: datetime, end_date: datetime, adj: bool):
    """Download trade data for a list of symbols and a given date range"""
    tasks = [asyncio.create_task(download_data_for_symbol(symbol, start_date, end_date, adj)) for symbol in symbol_list]
    data = await asyncio.gather(*tasks)
    return data


# Part 3: Main function to run everything

def main(symbol_list: List[str], start_date: str, end_date: str, adj: bool, folder: str) -> None:
    # Convert start and end dates to date objects
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()  # .strftime('%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    # Download data for all symbols in parallel
    all_data = asyncio.run(download_data_for_symbols(symbol_list, start_date, end_date, adj))

    # Write data to a single Parquet file per symbol, overwriting any existing file
    for symbol_data, symbol in zip(all_data, symbol_list):
        write_data_to_parquet(symbol_data, symbol, folder)


def get_repo_folder_path(*rel_path: str) -> str:
    repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    full_path = os.path.join(repo_path, *rel_path)
    os.makedirs(full_path, exist_ok=True)
    return full_path


if __name__ == "__main__":
    # List of symbols to download trade data for
    symbols = ["AAPL", "GOOG", "MSFT"]

    # Start and end dates for date range
    start = "2022-01-04"
    end = "2022-01-30"
    adjusted_values = True

    # Where to write the data
    folder_path = get_repo_folder_path('.data', '.raw')

    main(symbols, start, end, adjusted_values, folder_path)

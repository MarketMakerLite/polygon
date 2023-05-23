import os
import datetime
import asyncio
import aiohttp
from typing import Dict, Any, List
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import config

API_KEY = config.polygon_key


async def download_daily_open_close_data(symbol: str, date: str, adj: bool, session: aiohttp.ClientSession) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    base_url = 'https://api.polygon.io/v1/open-close/{stocksTicker}/{date}?adjusted={adj}'
    url = base_url.format(stocksTicker=symbol, date=date, adj=str(adj).lower())

    max_retries = 20
    retry_delay = 2

    for retry in range(max_retries):
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data
                elif resp.status == 404:
                    error_text = await resp.text()
                    if error_text.startswith('{"status":"NOT_FOUND"'):
                        print(f"No data found for {symbol} on {date}. Skipping.")
                        return {}
                    else:
                        raise aiohttp.ClientError(f"Error {resp.status}: {error_text}")
                else:
                    raise aiohttp.ClientError(f"Error {resp.status}: {await resp.text()}")
        except aiohttp.ClientError as e:
            if retry < max_retries - 1:
                wait_time = retry_delay * (2 ** retry)
                print(f"Error fetching data for {symbol} on {date}. Retrying in {wait_time} seconds. Error: {e}")
                await asyncio.sleep(wait_time)
            else:
                print(f"Failed to fetch data for {symbol} on {date} after {max_retries} attempts. Error: {e}")
                return {}


async def download_data_for_symbol(symbol: str, start_date: datetime, end_date: datetime, adj: bool,
                                   session: aiohttp.ClientSession, sem: asyncio.Semaphore, folder: str):
    symbol_data = []
    async with sem:
        for i in range((end_date - start_date).days + 1):
            date = (start_date + datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            data = await download_daily_open_close_data(symbol, date, adj, session)
            symbol_data.extend(data)

    # Write the data for the current symbol to disk
    write_data_to_parquet(symbol_data, symbol, folder)


async def download_data_for_symbols(symbol_list: List[str], start_date: datetime, end_date: datetime, adj: bool, folder: str):
    conn_limit = 10
    sem = asyncio.Semaphore(conn_limit)
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(download_data_for_symbol(symbol, start_date, end_date, adj, session, sem, folder)) for symbol in symbol_list]
        await asyncio.gather(*tasks)


def write_data_to_parquet(symbol_data: List[Dict[str, Any]], symbol: str, folder: str) -> None:
    df = pd.DataFrame(symbol_data)
    if not df.empty:
        table = pa.Table.from_pandas(df)

        # Create a subfolder based on the first character of the symbol
        subfolder = os.path.join(folder, symbol[0].upper())
        os.makedirs(subfolder, exist_ok=True)

        file_path = os.path.join(subfolder, f"{symbol}.parquet")

        # Remove existing file before writing the new data
        if os.path.exists(file_path):
            os.remove(file_path)

        pq.write_table(table, file_path, compression='snappy')


def main(symbol_list: List[str], start_date: str, end_date: str, adj: bool, folder: str) -> None:
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    # Download and save the data for all symbols in parallel
    asyncio.run(download_data_for_symbols(symbol_list, start_date, end_date, adj, folder))


def get_repo_folder_path(*rel_path: str) -> str:
    repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    full_path = os.path.join(repo_path, *rel_path)
    os.makedirs(full_path, exist_ok=True)
    return full_path


if __name__ == "__main__":
    symbols = ["AAPL", "GOOG", "MSFT"]
    start = "2012-12-05"
    end = "2023-03-16"
    adjusted_values = True
    folder_path = get_repo_folder_path('.data', '.raw')

    main(symbols, start, end, adjusted_values, folder_path)

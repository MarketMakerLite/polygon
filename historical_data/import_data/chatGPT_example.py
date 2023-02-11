import asyncio
import aiohttp
import concurrent.futures
import pyarrow as pa
import pyarrow.parquet as pq


async def download_data(session, url):
    """Asynchronously download data from the given URL"""
    async with session.get(url) as response:
        return await response.read()


async def download_data_for_day(date, api_key):
    """Asynchronously download trade data for the given date from Polygon.io"""
    base_url = 'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{date}/{date}/{interval}'
    """use a dictionary to store the arguments to the format function"""
    params = {
        'api_key': api_key,
        'interval': '1'  # Interval in minutes
    }
    async with aiohttp.ClientSession() as session:
        # Create tasks to download data for each ticker in parallel
        tasks = []
        for ticker in tickers:
            url = base_url.format(ticker=ticker, date=date, **params)
            task = asyncio.ensure_future(download_data(session, url))
            tasks.append(task)

        # Wait for all tasks to complete and retrieve results
        data = await asyncio.gather(*tasks)

        # Process and return data as needed
        return process_data(data)


def write_data_to_parquet(data, date):
    """Write the given data to a Parquet file for the given date"""
    # Convert data to a PyArrow Table
    table = pa.Table.from_pandas(data)

    # Write the table to a Parquet file
    pq.write_table(table, '{}.parquet'.format(date))


def download_and_write_data(dates, api_key):
    """Download and write trade data for the given dates using parallel asynchronous threads"""
    loop = asyncio.get_event_loop()

    # Create a thread pool with a fixed number of threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Create tasks to download data for each date in parallel
        tasks = []
        for date in dates:
            task = loop.run_in_executor(executor, download_data_for_day, date, api_key)
            tasks.append(task)

        # Wait for all tasks to complete and retrieve results
        data_by_day = loop.run_until_complete(asyncio.gather(*tasks))

        # Write data to Parquet files
        for date, data in zip(dates, data_by_day):
            write_data_to_parquet(data, date)


# Define the dates for which to download data
dates = ['2022-01-01', '2022-01-02', '2022-01-03']

# Download and write data using parallel asynchronous threads
download_and_write_data(dates, 'insertyourAPIkeyhere')

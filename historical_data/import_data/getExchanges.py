import asyncio
import aiohttp

async def get_exchanges():
    # Replace YOUR_API_KEY with your actual API key
    API_KEY = "insertyourAPIkeyhere"

    # Set the base URL for the Polygon API
    BASE_URL = "https://api.polygon.io"

    # Set the headers for the request
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{BASE_URL}/v3/reference/exchanges", headers=headers) as response:
            if response.status == 200:
                exchanges = await response.json()
                for exchange in exchanges["results"]:
                    print(exchange)
            else:
                print(f"Request failed with status code {response.status}")
                print(response.reason)

# Run the function asynchronously
asyncio.run(get_exchanges())


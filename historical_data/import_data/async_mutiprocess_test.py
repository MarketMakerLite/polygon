# code from https://ideone.com/lDVLFh originally
# io_bound/threaded.py
import concurrent.futures as futures
import requests
import threading
import time
import asyncio

data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]


async def async_task():
    for i in range(5):
        print(f"async running {i}")
        await asyncio.sleep(0.02)
    print("async done")
    return "I'm done"


def sleepy(n):
    time.sleep(1)
    return n * 2


async def ExecuteSleep():
    loop = asyncio.get_running_loop()
    l = len(data)
    results = []
    # Submit needs explicit mapping of I/p and O/p
    # Output may not be in the same Order
    with futures.ThreadPoolExecutor(max_workers=l) as executor:
        result_futures = {d: loop.run_in_executor(executor, sleepy, d) for d in data}
        results = {d: await result_futures[d] for d in data}

    return results


if __name__ == '__main__':
    print("Starting ...")
    t1 = time.time()

    loop = asyncio.get_event_loop()
    tasks = [ExecuteSleep(), async_task()]
    # x,y  = asyncio.run(asyncio.wait(tasks))
    x, y = loop.run_until_complete(asyncio.gather(*tasks))

    print(f"Printing x {x}")
    print(f"Printing y {y}")

    print("Finished ...")
    t2 = time.time()
    print(t2 - t1)
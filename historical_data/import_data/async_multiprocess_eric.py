# code from https://ideone.com/lDVLFh originally
# io_bound/threaded.py
import concurrent.futures as futures
import requests
import threading
import time
import asyncio

data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
background_tasks = set()

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

"""
from --- https://splunktool.com/python-callback-for-a-multiprocess-queue-or-pipe
4 ways of communicating back from a multi-process operation (no mention of futures?)

from --- https://stackoverflow.com/questions/61351844/difference-between-multiprocessing-asyncio-threading-and-concurrency-futures-i
use threading when you need to invoke blocking APIs in parallel, or precise control over threads
   when using threading, try to use concurrent.futures instead of creating them yourself
use multiprocessing - speed up CPU intensive tasks that can be run in parallel (threadsafe).
  disadvantage is you can't share in-memory datastructures
use concurrent.futures - to implement thread or process based parallelism
  use it when you want to work in parallel
asyncio - use this for all IO intensive operations
  use it when you want to wait in parallel
  can await functions executed in thread or process pools provided by concurrent futures, so it can glue modes together

from --- https://gist.github.com/ipwnponies/67082b5080cb3f951d07708d33200ad7
concurrent.futures employs time-slicing of CPU, where each thread gets a slice of time, but when it becomes IO bound yields
  If many threads become blocked for long periods, can degrade into polling (vs interrupt)
asyncio uses event loop and is more akin to a pub-sub, push notification model
  less concurrency than concurrent.futures as one thread can starve others if it has a lot of work to do
  if access pattern is threads that are blocked for a long time, this works well, since you don't check for a thread to 
    be ready to continue work, you wait for it to announce it's available.  
  
  
from --- https://stackoverflow.com/questions/54096301/can-concurrent-futures-future-be-converted-to-asyncio-future
Creating multiple event loops is somewhat of an antipattern in asyncio
https://stackoverflow.com/questions/54096301/can-concurrent-futures-future-be-converted-to-asyncio-future
"""

""" from -- https://docs.python.org/3/library/asyncio-task.html  Task Cancellation
It is recommended that coroutines use try/finally blocks to robustly perform clean-up logic.
In case asyncio.CancelledError is explicitly caught, it should generally be propagated when clean-up is complete. 
Most code can safely ignore asyncio.CancelledError.
"""


async def say_after(delay, what):
    try:
        await asyncio.sleep(delay)
    finally:
        print('Cleanup objects here')


async def main():
    # tasks = [ExecuteSleep(), async_task()]
    # something = await asyncio.gather(*tasks)
    # print(f"Printing results {something}")

    # NOTE: If any object in work-to-do is a coroutine,
    # the asyncio.gather() function will automatically schedule it as a task.
    # x, y = await asyncio.gather(ExecuteSleep(), async_task(), return_exceptions=True)
    # print(f"Printing x {x}")
    # print(f"Printing y {y}")

    print(f"started at {time.strftime('%X')}")

    task1 = asyncio.create_task(say_after(3, 'hello'))
    task2 = asyncio.create_task(say_after(1, 'world'))

    background_tasks.add(task1)
    task1.add_done_callback(background_tasks.discard)
    background_tasks.add(task2)
    task2.add_done_callback(background_tasks.discard)

    """
    await task1
    await task2
    """

    # NEW - use asyncio.gather to schedule several coroutines concurrently
    await asyncio.gather(task1, task2)
    print(f"finished at {time.strftime('%X')}")

    """ NEWEST:: concurrent coroutine scheduling syntax for 3.11 and above 
    async with asyncio.TaskGroup() as tg:
        task1 = tg.create_task(say_after(3, 'hello'))
        task2 = tg.create_task(say_after(1, 'world'))
        print(f"started at {time.strftime('%X')}")

    # the scheduling and waiting for tasks to complete is implicit when leaving the "with" block
    print(f"finished at {time.strftime('%X')}")
    """

if __name__ == '__main__':
    start_time = time.time()
    print(f"Starting at {start_time}...")
    asyncio.run(main())
    print(f'Finished in {time.time() - start_time}')

""" BEST PRACTICE for 3.10 and later
ref: https://docs.python.org/3/library/asyncio-task.html

advice from https://blog.teclado.com/changes-to-async-event-loops-in-python-3-10/
1) Define a "main" coroutine that is the entry point to your asynchronous code. Here we are doing async def main().
2) Instead of calling get_event_loop and run_until_complete, just call asyncio.run(main()).
   Doing so begins a loop and runs it until complete, so a call to get_running_loop() will always have a result
   and functions like asyncio.wrap_future() which have an optional Loop parameter will always be satisfied

advice from https://stackoverflow.com/questions/61351844/difference-between-multiprocessing-asyncio-threading-and-concurrency-futures-i
asyncio can await functions executed in thread or process pools provided by concurrent futures, 
so use it when you need to glue together asyncio and threading/process pooling paradigms
"""

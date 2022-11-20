from concurrent.futures.thread import ThreadPoolExecutor
# from concurrent.futures import ProcessPoolExecutor
from time import sleep
# Comment out asyncio import for testing threads, it will mess with threading
import asyncio
import datetime
import random
from typing import List, Any, Tuple


class AsyncThreadPool(ThreadPoolExecutor):
    _futures: List[asyncio.Future]
    _loop: asyncio.AbstractEventLoop

    def __init__(self, max_workers=None):
        super().__init__(max_workers)
        self._futures = []

    def queue(self, fn):
        self._loop = asyncio.get_event_loop()
        fut = self._loop.create_future()
        self._futures.append(fut)
        self.submit(self._entry, fn, fut)

    def queue_async(self, coroutine):
        def new_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coroutine)

        self.queue(new_loop)

    def _entry(self, fn, fut: asyncio.Future):
        try:
            result = fn()
            self._loop.call_soon_threadsafe(fut.set_result, result)
        except Exception as e:
            self._loop.call_soon_threadsafe(fut.set_exception, e)

    async def gather(self) -> tuple[BaseException | Any, BaseException | Any, BaseException | Any, BaseException | Any,
                                    BaseException | Any]:
        # return await asyncio.gather(*self._futures)
        return await asyncio.gather(*self._futures)


def return_after_n_secs(message, time=2):
    print(f'Starting {message}')
    sleep(time)
    print(f'Finished {message}')
    return message


def concurrent_test():
    # pool = ProcessPoolExecutor ??
    pool = ThreadPoolExecutor(3)

    future = pool.submit(return_after_n_secs, "hello", 5)
    future1 = pool.submit(return_after_n_secs, "world", 2)
    sleep(5)
    print(future.result())
    print(future1.result())


async def display_date(num, time):
    print(f"Starting Loop: {num} Time: {datetime.datetime.now().time()}")
    # time = random.randint(3, 5)
    print(f'Processing.... taking {time} seconds')
    sleep(2)
    print(f'Took 2 seconds of processing, now blocking on io {datetime.datetime.now().time()}')

    await asyncio.sleep(time)
    print('Finished loop {loop} {time}'.format(time=datetime.datetime.now().time(), loop=num))
    # await my_sleep_func(time)


def asyncio_test():
    loop = asyncio.get_event_loop()

    asyncio.ensure_future(display_date(1, 10))
    asyncio.ensure_future(display_date(2, 5))

    loop.run_forever()


async def stack_test():
    with AsyncThreadPool(2) as pool:
        # Queue some sync function (will be executed on another thread)
        pool.queue(return_after_n_secs("hello", 5))
        # Queue a coroutine that will be executed on a new event loop running on another thread
        pool.queue(display_date(1, 7))

        # Gather results (non blocking for your current loop)
        res: List[Any] = await pool.gather()


if __name__ == '__main__':
    # concurrent_test()
    # asyncio_test()
    asyncio.run(stack_test())

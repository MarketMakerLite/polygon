import asyncio, time


async def fn1(x):
    await asyncio.sleep(3)
    print(f"fn1 is called with x={x}")
    return f"Hello from fn1: {x}"


async def raise_error(y):
    await asyncio.sleep(2)
    print(f"raise_error is called with y={y}")
    raise asyncio.TimeoutError()  # <- function exits here
    # no result
    return f"Hello from raise_error"


async def main(return_exceptions):
    print("start:", time.ctime())
    print(f"return_exceptions={return_exceptions}")

    if return_exceptions:
        result2 = await asyncio.gather(
            fn1("First call"),  # <- 1st result
            raise_error("Second call"),  # <- here is the error
            fn1("Third call"),  # <- 3rd result will be evaluated as well
            return_exceptions=return_exceptions,  # <- return exceptions as results
        )
        print("result2:", result2)
        await asyncio.sleep(2)
        print("end:", time.ctime())
    else:
        try:
            result1 = await asyncio.gather(
                fn1("First call"),  # <- 1st result
                raise_error("Second call"),  # <- here is the error
                fn1("Third call"),  # <- 3rd result
                return_exceptions=return_exceptions,  # <- don't return result, if an exception was raised
                # but tasks are not canceled. You can see it with the print function, that
                # fn1 is still called, but you won't get a return value from gather
            )
        except Exception as e:
            print("Caught Exception from gather")
            print("We don't get the result back, but the tasks are not canceled")

        # result1 does not exist
        try:
            print('Result1', result1)
        except NameError:
            print('As expected you get a NameError, result1 does not exist')


if __name__ == '__main__':
    start_time = time.time()
    print(f"Starting at {start_time}...")
    asyncio.run(main(False))
    print()
    time.sleep(1)
    print()
    asyncio.run(main(True))
    print(f'Finished in {time.time() - start_time}')

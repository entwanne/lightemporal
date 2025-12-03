import time

from lightemporal import executor
from tasks.exceptions import Suspend
from tasks.queue import ENV


def worker(**workflows):
    executor.CTX = executor.TaskExecutorContext()

    while True:
        queue = ENV['Q']
        func, args, kwargs = queue.get(workflows)
        print(func, args, kwargs)
        try:
            print(func(*args, **kwargs))
        except Suspend as e:
            print(f'{func.__name__} suspended for {round(max(e.timestamp - time.time(), 0))}s')
            queue.call_at(func, e.timestamp, *args, **kwargs)
        except Exception as e:
            print(f'{func.__name__} failed: {e!r}')
            queue.put(func, *args, **kwargs)

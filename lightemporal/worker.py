import time

from lightemporal import executor
from tasks.exceptions import Suspend
from tasks.queue import Q


def worker(**workflows):
    executor.CTX = executor.TaskExecutorContext()

    while True:
        func, args, kwargs = Q.get(workflows)
        print(func, args, kwargs)
        try:
            print(func(*args, **kwargs))
        except Suspend as e:
            print(f'{func.__name__} suspended for {round(max(e.timestamp - time.time(), 0))}s')
            Q.call_at(func, e.timestamp, *args, **kwargs)
        except Exception as e:
            print(f'{func.__name__} failed: {e!r}')
            Q.put(func, *args, **kwargs)

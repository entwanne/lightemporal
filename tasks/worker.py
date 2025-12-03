import time

from lightemporal.context import ENV

from .exceptions import Suspend
from .shared import functions


def run(**functions):
    queue = ENV['Q']
    while True:
        func, args, kwargs = queue.get(functions)
        print(func, args, kwargs)
        try:
            print(func(*args, **kwargs))
        except Suspend as e:
            print(f'{func} suspended for {round(max(e.timestamp - time.time(), 0))}s')
            queue.call_at(func, e.timestamp, *args, **kwargs)


if __name__ == '__main__':
    run(**functions)

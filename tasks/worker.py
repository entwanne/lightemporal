import time

from .exceptions import Suspend
from .queue import Q
from .shared import functions


def run(**functions):
    while True:
        func, args, kwargs = Q.get(functions)
        print(func, args, kwargs)
        try:
            print(func(*args, **kwargs))
        except Suspend as e:
            print(f'{func} suspended for {round(max(e.timestamp - time.time(), 0))}s')
            Q.call_at(func, e.timestamp, *args, **kwargs)


if __name__ == '__main__':
    run(**functions)

import time

from lightemporal.context import ENV

from .exceptions import Suspend
from .shared import functions


def run(**functions):
    queue = ENV['Q']
    while True:
        task_id, func, args, kwargs = queue.get(functions)
        print(func, args, kwargs)
        try:
            ret = func(*args, **kwargs)
            print(ret)
        except Suspend as e:
            print(f'{func} suspended for {round(max(e.timestamp - time.time(), 0))}s')
            queue._call(task_id, func, e.timestamp, args, kwargs)
        else:
            queue.set_result(task_id, func, ret)


if __name__ == '__main__':
    run(**functions)

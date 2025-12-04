import sys
import time
from importlib.metadata import EntryPoint

from ..core.context import ENV

from .exceptions import Suspend


def run_worker(**functions):
    queue = ENV['Q']

    while True:
        task_id, func, args, kwargs = queue.get(functions)
        print(func, args, kwargs)

        try:
            ret = func(*args, **kwargs)
            print(ret)
        except Suspend as e:
            print(f'{func.__name__} suspended for {round(max(e.timestamp - time.time(), 0))}s')
            queue._call(task_id, func, e.timestamp, args, kwargs)
        except Exception as e:
            print(f'{func.__name__} failed: {e!r}')
            queue._call(task_id, func, time.time(), args, kwargs)
        else:
            queue.set_result(task_id, func, ret)


def load_functions(*names):
    funcs = (EntryPoint(name, value=name, group='tasks').load() for name in names)
    return {f.__name__: f for f in funcs}


if __name__ == '__main__':
    functions = load_functions(*sys.argv[1:])
    run_worker(**functions)

import sys
import time
from importlib.metadata import EntryPoint

from ..core.context import ENV

from .exceptions import Suspend
from .retry import DEFAULT_POLICY


def run_worker(retry_policy=DEFAULT_POLICY, /, **functions):
    queue = ENV['Q']

    while True:
        task_id, func, retry_count, args, kwargs = queue.get(functions)
        print(func, args, kwargs)

        try:
            ret = func(*args, **kwargs)
            print(ret)
        except Suspend as e:
            print(f'{func.__name__} suspended for {round(max(e.timestamp - time.time(), 0))}s')
            queue._call(task_id, func, e.timestamp, retry_count, args, kwargs)
        except retry_policy.error_type as e:
            print(f'{func.__name__} failed: {e!r}')
            if retry_count < retry_policy.max_retries:
                delay = retry_policy.delay * retry_policy.backoff ** retry_count
                print(f'Retrying in {delay}s')
                queue._call(task_id, func, time.time() + delay, retry_count + 1, args, kwargs)
            else:
                queue.set_error(task_id, str(e))
        else:
            queue.set_result(task_id, func, ret)


def load_functions(*names):
    funcs = (EntryPoint(name, value=name, group='tasks').load() for name in names)
    return {f.__name__: f for f in funcs}


if __name__ == '__main__':
    functions = load_functions(*sys.argv[1:])
    run_worker(**functions)

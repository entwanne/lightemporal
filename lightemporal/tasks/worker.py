import sys
import time
import types
from importlib.metadata import EntryPoint

from ..core.context import ENV

from .discovery import load, get_task_name, discover_from_names
from .exceptions import Suspend
from .retry import DEFAULT_POLICY


def run_worker(retry_policy=DEFAULT_POLICY, /, **tasks):
    queue = ENV['Q']

    for name, func in sorted(tasks.items()):
        print('Loaded', name, ':', func)

    while True:
        task_id, func, retry_count, args, kwargs = queue.get(tasks)
        task_name = get_task_name(func)
        print(func, args, kwargs)

        try:
            ret = func(*args, **kwargs)
            print(repr(ret))
        except Suspend as e:
            if e.timestamp is None:
                print(f'{task_name} suspended')
                queue._postpone(task_id, func, retry_count, args, kwargs)
            else:
                print(f'{task_name} suspended for {round(max(e.timestamp - time.time(), 0))}s')
                queue._call(task_id, func, e.timestamp, retry_count, args, kwargs)
        except retry_policy.error_type as e:
            print(f'{task_name} failed: {e!r}')
            if retry_count < retry_policy.max_retries:
                delay = retry_policy.delay * retry_policy.backoff ** retry_count
                print(f'Retrying in {delay}s')
                queue._call(task_id, func, time.time() + delay, retry_count + 1, args, kwargs)
            else:
                queue.set_error(task_id, str(e))
        else:
            queue.set_result(task_id, func, ret)


def run(retry_policy=DEFAULT_POLICY, /, **tasks):
    run_worker(retry_policy, **load(), **tasks)


if __name__ == '__main__':
    run(**discover_from_names(*sys.argv[1:]))

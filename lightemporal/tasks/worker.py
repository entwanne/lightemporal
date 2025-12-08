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
        task = queue.get(tasks)
        print(task.func, task.args, task.kwargs)

        try:
            ret = task.func(*task.args, **task.kwargs)
            print(repr(ret))
        except Suspend as e:
            if e.timestamp is None:
                print(f'{task.name} suspended')
                queue.suspend(task)
            else:
                print(f'{task.name} suspended for {round(max(e.timestamp - time.time(), 0))}s')
                queue.put(task.later(timestamp=e.timestamp))
        except retry_policy.error_type as e:
            print(f'{task.name} failed: {e!r}')
            if task.retry_count < retry_policy.max_retries:
                delay = retry_policy.delay * retry_policy.backoff ** task.retry_count
                print(f'Retrying in {delay}s')
                queue.put(task.retry(delay=delay))
            else:
                queue.set_error(task, str(e))
        else:
            queue.set_result(task, ret)


def run(retry_policy=DEFAULT_POLICY, /, **tasks):
    run_worker(retry_policy, **load(), **tasks)


if __name__ == '__main__':
    run(**discover_from_names(*sys.argv[1:]))

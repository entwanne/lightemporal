import sys
import time
import types
from importlib.metadata import EntryPoint

from ..core.context import ENV

from .exceptions import Suspend
from .retry import DEFAULT_POLICY
from .utils import get_full_name


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


def discover_functions(*args):
    def _discover(basename, obj):
        try:
            full_name = get_full_name(obj)
        except AttributeError:
            return
        if not full_name.startswith(basename):
            return

        if hasattr(obj, '__call__'):
            print('Loaded', full_name, ':', obj)
            functions[full_name] = obj

        if isinstance(obj, (type, types.ModuleType)) and (attrs := getattr(obj, '__dict__', None)):
            for name, attr in attrs.items():
                if name.startswith('__') and name.endswith('__'):
                    continue
                _discover(full_name, attr)

    functions = {}

    for arg in args:
        _discover('', arg)

    return functions


def run(*args, **kwargs):
    return run_worker(**discover_functions(*args), **kwargs)


def discover_entrypoints(*names):
    return discover_functions(*(EntryPoint(name, value=name, group='tasks').load() for name in names))


if __name__ == '__main__':
    functions = discover_entrypoints(*sys.argv[1:])
    run_worker(**functions)

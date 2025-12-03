import time
from contextlib import contextmanager

from tasks.exceptions import Suspend

from .context import ENV
from .workflow import Runner as DefaultRunner


class Runner:
    def call(self, workflow, *args, **kwargs):
        ENV['Q'].put(workflow, *args, **kwargs)


class TaskExecution:
    def suspend(self, timestamp):
        raise Suspend(timestamp=timestamp)


def worker(**workflows):
    with ENV.new_layer():
        ENV['EXEC'] = TaskExecution()
        ENV['RUN'] = DefaultRunner()

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


@contextmanager
def task_runner():
    with ENV.new_layer():
        ENV['RUN'] = Runner()
        yield

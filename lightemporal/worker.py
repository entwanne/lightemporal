import time
from contextlib import contextmanager

from tasks.exceptions import Suspend

from .context import ENV
from .workflow import Runner as DefaultRunner


class Runner:
    def call(self, workflow, *args, **kwargs):
        return ENV['Q'].execute(workflow, *args, **kwargs)


class TaskExecution:
    def suspend(self, timestamp):
        raise Suspend(timestamp=timestamp)


def worker(**workflows):
    with ENV.new_layer():
        ENV['EXEC'] = TaskExecution()
        ENV['RUN'] = DefaultRunner()

        while True:
            queue = ENV['Q']
            task_id, func, args, kwargs = queue.get(workflows)
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


@contextmanager
def task_runner():
    with ENV.new_layer():
        ENV['RUN'] = Runner()
        yield

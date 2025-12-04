import time
from contextlib import contextmanager

from .core.context import ENV
from .tasks.exceptions import Suspend
from .workflow import Runner as DefaultRunner


class Runner:
    def start(self, workflow, *args, **kwargs):
        task_id = ENV['Q'].put(workflow, *args, **kwargs)
        # + get intermediate result from the task

    def call(self, workflow, *args, **kwargs):
        return ENV['Q'].execute(workflow, *args, **kwargs)


class TaskExecution:
    def suspend(self, timestamp):
        raise Suspend(timestamp=timestamp)


@contextmanager
def worker_env():
    with ENV.new_layer():
        ENV['EXEC'] = TaskExecution()
        ENV['RUN'] = DefaultRunner()
        yield


@contextmanager
def runner_env():
    with ENV.new_layer():
        ENV['RUN'] = Runner()
        yield

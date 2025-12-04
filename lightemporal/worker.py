import time
from contextlib import contextmanager

from .core.context import ENV
from .tasks.discovery import get_task_name
from .tasks.exceptions import Suspend
from .workflow import Runner as DefaultRunner, workflow


class Runner:
    def start(self, workflow, *args, **kwargs):
        task_id = ENV['Q'].put(workflow.start, *args, **kwargs)
        # + get intermediate result from the task

    def call(self, workflow, *args, **kwargs):
        return ENV['Q'].execute(workflow.run, *args, **kwargs)


class TaskExecution:
    def suspend(self, timestamp):
        raise Suspend(timestamp=timestamp)


def decorate_workflows():
    class MethodWrapper:
        def __init__(self, target, **kwargs):
            self.target = target
            self.__dict__.update(kwargs)

        def __call__(self, *args, **kwargs):
            return self.target(*args, **kwargs)

    for w in workflow.instances:
        w.__module__ = w.func.__module__
        w.__name__ = w.func.__name__
        w.__qualname__ = w.func.__qualname__
        w.__taskname__ = get_task_name(w.func)

        w.start = MethodWrapper(
            w.start,
            __taskname__=w.__taskname__+'.start',
            __signature__=w.sig,
        )
        w.run = MethodWrapper(
            w.start,
            __taskname__=w.__taskname__+'.run',
            __signature__=w.sig,
        )



@contextmanager
def worker_env():
    with ENV.new_layer():
        ENV['EXEC'] = TaskExecution()
        ENV['RUN'] = DefaultRunner()
        decorate_workflows()
        yield


@contextmanager
def runner_env():
    with ENV.new_layer():
        ENV['RUN'] = Runner()
        decorate_workflows()
        yield


def discover_tasks_from_workflow(workflow):
    name = get_task_name(workflow)
    return {
        get_task_name(workflow.start): workflow.start,
        get_task_name(workflow.run): workflow.run,
    }


def discover_tasks_from_workflows(*workflows):
    return {
        name: task
        for workflow in workflows
        for name, task in discover_tasks_from_workflow(workflow).items()
    }

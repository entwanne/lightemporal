import inspect
import time
from contextlib import contextmanager
from functools import cached_property

from .core.context import ENV
from .tasks.discovery import get_task_name
from .tasks.exceptions import Suspend
from .workflow import workflow


class TaskExecution:
    def suspend_until(self, workflow_id, timestamp):
        raise Suspend(timestamp=timestamp)

    def suspend(self, workflow_id):
        raise Suspend


class TaskRunner:
    @cached_property
    def workflow_table(self):
        return ENV['DB'].tables['tasks.workflows']

    def start(self, workflow, *args, **kwargs):
        workflow_id = ENV['Q'].execute(workflow._create, *args, **kwargs)
        task = ENV['Q'].call(workflow._run, workflow_id)
        self.workflow_table.set({'id': workflow_id, 'task_id': task.id})
        return Handler(workflow, workflow_id, task.id)

    def run(self, workflow, *args, **kwargs):
        return ENV['Q'].execute(workflow.run, *args, **kwargs)

    def wake_up(self, workflow_id):
        data = self.workflow_table.get(workflow_id)
        ENV['Q'].wakeup(data['task_id'])


class Handler:
    def __init__(self, workflow, workflow_id, task_id):
        self.workflow = workflow
        self.workflow_id = workflow_id
        self.task_id = task_id

    def result(self):
        return ENV['Q'].get_result(self.workflow, self.task_id)


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

        w._create = MethodWrapper(
            w._create,
            __taskname__=w.__taskname__+'._create',
            __signature__=w.__signature__.replace(return_annotation=str),
        )
        w._run = MethodWrapper(
            w._run,
            __taskname__=w.__taskname__+'._run',
            __signature__=inspect.signature(w._run).replace(return_annotation=w.__signature__.return_annotation),
        )
        w.run = MethodWrapper(
            w.run,
            __taskname__=w.__taskname__+'.run',
            __signature__=w.sig,
        )



@contextmanager
def worker_env():
    with ENV.new_layer():
        decorate_workflows()
        ENV['EXEC'] = TaskExecution()
        ENV['RUN'] = TaskRunner()
        yield


@contextmanager
def runner_env():
    with ENV.new_layer():
        decorate_workflows()
        ENV['RUN'] = TaskRunner()
        yield


def discover_tasks_from_workflow(workflow):
    name = get_task_name(workflow)
    return {
        get_task_name(workflow._create): workflow._create,
        get_task_name(workflow._run): workflow._run,
        get_task_name(workflow.run): workflow.run,
    }


def discover_tasks_from_workflows(*workflows):
    return {
        name: task
        for workflow in workflows
        for name, task in discover_tasks_from_workflow(workflow).items()
    }

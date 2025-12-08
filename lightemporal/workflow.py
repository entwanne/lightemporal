import contextvars
import inspect
import time
from contextlib import contextmanager

from .core.context import ENV
from .core.utils import SignatureWrapper
from .models import Workflow, WorkflowStatus, Activity, Signal
from .repos import Repositories

repos = Repositories()


class workflow:
    instances = []
    currents = contextvars.ContextVar('current_workflows', default=())

    def __init__(self, func):
        self.func = func
        self.name = func.__qualname__
        self.sig = SignatureWrapper.from_function(func)
        self.__signature__ = self.sig.signature

        self.instances.append(self)

    def run(self, *args, **kwargs):
        return ENV['RUN'].run(self, *args, **kwargs)

    def start(self, *args, **kwargs):
        return ENV['RUN'].start(self, *args, **kwargs)

    def __call__(self, *args, **kwargs):
        return ENV['RUN'].run(self, *args, **kwargs)

    def _create(self, *args, **kwargs):
        input_str = self.sig.dump_input(*args, **kwargs)
        workflow = repos.workflows.get_or_create(self.name, input_str)
        return workflow.id

    def _run(self, workflow_id: str):
        workflow = repos.workflows.get(workflow_id)
        print(repr(workflow))

        args, kwargs = self.sig.load_input(workflow.input)
        self.currents.set(self.currents.get() + ({'id': workflow.id, 'step': 0},))

        exc = False

        try:
            return self.func(*args, **kwargs)
        except Exception:
            exc = True
            raise
        finally:
            assert self.currents.get()[-1]['id'] == workflow.id
            self.currents.set(self.currents.get()[:-1])
            if exc:
                repos.workflows.failed(workflow)
            else:
                repos.workflows.complete(workflow)

    @contextmanager
    def use(self, *args, **kwargs):
        input_str = self.sig.dump_input(*args, **kwargs)
        workflow = repos.workflows.get_or_create(self.name, input_str)
        try:
            yield
        finally:
            repos.workflows.complete(workflow)

    @staticmethod
    def sleep(duration):
        return _sleep_until(_timestamp_for_duration(duration))

    @staticmethod
    def wait(signal_cls):
        while True:
            workflow_ctx = workflow.currents.get()[-1]
            workflow_id = workflow_ctx['id']
            workflow_ctx['step'] += 1
            step = workflow_ctx['step']
            if signal := repos.signals.may_find_one(workflow_id, signal_cls.__signal_name__, step):
                return signal_cls.model_validate(signal.content)
            ENV['EXEC'].suspend(workflow_id)

    @staticmethod
    def signal(workflow_id: str, signal):
        repos.signals.new(Signal(
            workflow_id=workflow_id,
            name=type(signal).__signal_name__,
            content=signal.model_dump(mode='json'),
        ))
        ENV['RUN'].wake_up(workflow_id)


class activity:
    def __init__(self, func):
        self.func = func
        self.name = func.__qualname__
        self.sig = SignatureWrapper.from_function(func)

    def __call__(self, *args, **kwargs):
        if not workflow.currents.get():
            raise ValueError('No current workflow')
        workflow_ctx = workflow.currents.get()[-1]
        workflow_id = workflow_ctx['id']
        exc = None

        input_str = self.sig.dump_input(*args, **kwargs)

        workflow_ctx['step'] += 1
        name = f'{self.name}#{workflow_ctx['step']}'
        activity = repos.activities.may_find_one(workflow_id, name, input_str)
        if activity is not None:
            return self.sig.load_output(activity.output)

        try:
            ret = self.func(*args, **kwargs)
            return ret
        except Exception:
            exc = True
            raise
        finally:
            if not exc:
                output_str = self.sig.dump_output(ret)
                activity = Activity(workflow_id=workflow_id, name=name, input=input_str, output=output_str)
                repos.activities.save(activity)


@activity
def _timestamp_for_duration(duration: int) -> float:
    return time.time() + duration


@activity
def _sleep_until(timestamp: float) -> None:
    workflow_ctx = workflow.currents.get()[-1]
    workflow_id = workflow_ctx['id']
    if timestamp > time.time():
        ENV['EXEC'].suspend_until(workflow_id, timestamp)


def signal(f):
    f.__signal_name__ = f.__name__
    return f

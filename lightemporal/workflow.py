import contextvars
import inspect
import time
from contextlib import contextmanager

import pydantic

from .core.context import ENV
from .core.utils import SignatureWrapper
from .models import Workflow, WorkflowStatus, Activity, Signal
from .repos import Repositories

repos = Repositories()


class WorkflowContext(pydantic.BaseModel):
    id: str
    step: int = 0

    def next_step(self):
        self.step += 1
        return self.step


class workflow:
    instances = []
    _currents = contextvars.ContextVar('current_workflows', default=())

    def __init__(self, func):
        self.func = func
        self.name = func.__qualname__
        self.sig = SignatureWrapper.from_function(func)
        self.__signature__ = self.sig.signature

        self.instances.append(self)

    @classmethod
    def _current(cls):
        currents = cls._currents.get()
        if not currents:
            raise ValueError('No current workflow')
        return currents[-1]

    @classmethod
    def _enter_workflow(cls, workflow):
        cls._currents.set((*cls._currents.get(), WorkflowContext(id=workflow.id)))

    @classmethod
    def _exit_workflow(cls, workflow):
        assert cls._currents.get()[-1].id == workflow.id
        cls._currents.set(cls._currents.get()[:-1])

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
        self._enter_workflow(workflow)

        exc = False

        try:
            return self.func(*args, **kwargs)
        except Exception:
            exc = True
            raise
        finally:
            self._exit_workflow(workflow)
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

    @classmethod
    def wait(cls, signal_cls):
        while True:
            workflow_ctx = cls._current()
            if signal := repos.signals.may_find_one(workflow_ctx.id, signal_cls.__signal_name__, workflow_ctx.next_step()):
                return signal_cls.model_validate(signal.content)
            ENV['EXEC'].suspend(workflow_ctx.id)

    @classmethod
    def on(cls, signal_cls, handler):
        # register an handler to be executed async when the expected signal is received
        RUN['EXEC'].on_signal(signal_cls)
        # -> computes the step for waiting the signal
        # -> trigger a thread to call handler(wait(signal_cls)) (with step computed at the previous step)
        # -> return a handler to join the async task / thread
        # -> may be used as a context manager to unregister handler
        pass

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
        workflow_ctx = workflow._current()
        exc = None

        input_str = self.sig.dump_input(*args, **kwargs)

        name = f'{self.name}#{workflow_ctx.next_step()}'
        activity = repos.activities.may_find_one(workflow_ctx.id, name, input_str)
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
                activity = Activity(workflow_id=workflow_ctx.id, name=name, input=input_str, output=output_str)
                repos.activities.save(activity)


@activity
def _timestamp_for_duration(duration: int) -> float:
    return time.time() + duration


@activity
def _sleep_until(timestamp: float) -> None:
    if timestamp > time.time():
        ENV['EXEC'].suspend_until(workflow._current().id, timestamp)


def signal(f):
    f.__signal_name__ = f.__name__
    return f

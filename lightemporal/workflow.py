import contextvars
import inspect
import threading
from contextlib import contextmanager

import pydantic

from .core.context import ENV
from .core.utils import param_types
from .models import Workflow, WorkflowStatus, Activity, Signal
from .repos import Repositories

repos = Repositories()


class DirectRunner:
    def start(self, workflow, *args, **kwargs):
        raise RuntimeError('Cannot start async workflow with direct runner')

    def run(self, workflow, *args, **kwargs):
        w_id = workflow._create(*args, **kwargs)
        return workflow._run(w_id)


class Handler:
    unset = object()

    def __init__(self, target, workflow_id):
        self._target = target
        self.workflow_id = workflow_id
        self.thread = threading.Thread(target=self.target, args=(dict(ENV),))
        self.ret = self.unset
        self.error = self.unset

    def target(self, parent_env):
        try:
            with ENV.new_layer():
                ENV.update(parent_env)
                self.ret = self._target(self.workflow_id)
        except Exception as e:
            self.error = e

    def start(self):
        self.thread.start()

    def result(self):
        self.thread.join()
        if self.error is not self.unset:
            raise self.error
        assert self.ret is not self.unset
        return self.ret


class ThreadRunner:
    def start(self, workflow, *args, **kwargs):
        handler = Handler(workflow._run, workflow._create(*args, **kwargs))
        handler.start()
        return handler

    def run(self, workflow, *args, **kwargs):
        handler = self.start(workflow, *args, **kwargs)
        return handler.result()


ENV['RUN'] = DirectRunner()
#ENV['RUN'] = ThreadRunner()


class workflow:
    instances = []
    currents = contextvars.ContextVar('current_workflows', default=())

    def __init__(self, func):
        self.func = func
        self.name = func.__qualname__
        self.sig = self.__signature__ = inspect.signature(func)
        self.arg_types, self.kwarg_types = param_types(func)
        self.input_adapter = pydantic.TypeAdapter(tuple[self.arg_types, self.kwarg_types])

        self.instances.append(self)

    def run(self, *args, **kwargs):
        return ENV['RUN'].run(self, *args, **kwargs)

    def start(self, *args, **kwargs):
        return ENV['RUN'].start(self, *args, **kwargs)

    def __call__(self, *args, **kwargs):
        return ENV['RUN'].run(self, *args, **kwargs)

    def _create(self, *args, **kwargs):
        bound = self.sig.bind(*args, **kwargs)
        args, kwargs = bound.args, bound.kwargs
        kwargs = self.kwarg_types(**kwargs)
        input_str = self.input_adapter.dump_json((args, kwargs)).decode()
        workflow = repos.workflows.get_or_create(self.name, input_str)
        return workflow.id

    def _run(self, workflow_id: str):
        workflow = repos.workflows.get(workflow_id)
        print(repr(workflow))

        args, kwargs = self.input_adapter.validate_json(workflow.input)
        self.currents.set(self.currents.get() + ({'id': workflow.id, 'step': 0},))

        exc = False

        try:
            return self.func(*args, **kwargs.model_dump())
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
        bound = self.sig.bind(*args, **kwargs)
        args, kwargs = bound.args, bound.kwargs
        kwargs = self.kwarg_types(**kwargs)
        input_str = self.input_adapter.dump_json((args, kwargs)).decode()
        workflow = repos.workflows.get_or_create(self.name, input_str)
        try:
            yield
        finally:
            repos.workflows.complete(workflow)

    @staticmethod
    def sleep(duration):
        from .executor import _sleep_until, _timestamp_for_duration
        return _sleep_until(_timestamp_for_duration(duration))

    @staticmethod
    def wait(name: str):
        workflow_ctx = workflow.currents.get()[-1]
        workflow_id = workflow_ctx['id']
        workflow_ctx['step'] += 1
        step = workflow_ctx['step']
        if repos.signals.may_find_one(workflow_id, name, step):
            return
        ENV['EXEC'].suspend()

    @staticmethod
    def signal(workflow_id: str, name: str):
        repos.signals.new(Signal(workflow_id=workflow_id, name=name))
        ENV['EXEC'].wake_up(workflow_id)


class activity:
    def __init__(self, func):
        self.func = func
        self.name = func.__qualname__
        self.sig = inspect.signature(func)
        self.arg_types, self.kwarg_types = param_types(func)
        self.input_adapter = pydantic.TypeAdapter(tuple[self.arg_types, self.kwarg_types])
        self.output_adapter = pydantic.TypeAdapter(self.sig.return_annotation)

    def __call__(self, *args, **kwargs):
        if not workflow.currents.get():
            raise ValueError('No current workflow')
        workflow_ctx = workflow.currents.get()[-1]
        workflow_id = workflow_ctx['id']
        exc = None

        bound = self.sig.bind(*args, **kwargs)
        args, kwargs = bound.args, bound.kwargs
        kwargs = self.kwarg_types(**kwargs)
        input_str = self.input_adapter.dump_json((args, kwargs)).decode()

        workflow_ctx['step'] += 1
        name = f'{self.name}#{workflow_ctx['step']}'
        activity = repos.activities.may_find_one(workflow_id, name, input_str)
        if activity is not None:
            return self.output_adapter.validate_json(activity.output)

        try:
            ret = self.func(*args, **kwargs.model_dump())
            return ret
        except Exception:
            exc = True
            raise
        finally:
            if not exc:
                output_str = self.output_adapter.dump_json(ret).decode()
                activity = Activity(workflow_id=workflow_id, name=name, input=input_str, output=output_str)
                repos.activities.save(activity)

import contextvars
import inspect
from contextlib import contextmanager

import pydantic

from .backend import DB
from .models import param_types, Workflow, WorkflowStatus, Activity
from .repos import WorkflowRepository, ActivityRepository


workflows = WorkflowRepository(DB)
activities = ActivityRepository(DB)


class workflow:
    currents = contextvars.ContextVar('current_workflows', default=())

    def __init__(self, func):
        self.func = func
        self.name = func.__qualname__
        self.sig = inspect.signature(func)
        #self.input_adapter = params_adapter(func)
        self.arg_types, self.kwarg_types = param_types(func)
        self.input_adapter = pydantic.TypeAdapter(tuple[self.arg_types, self.kwarg_types])

    def __call__(self, *args, **kwargs):
        exc = False
        bound = self.sig.bind(*args, **kwargs)
        args, kwargs = bound.args, bound.kwargs
        kwargs = self.kwarg_types(**kwargs)
        #print(repr(kwargs), kwargs.model_dump())
        #print(b.args, b.kwargs)
        #user_list_adapter = pydantic.TypeAdapter(tuple[tuple[str]])
        input_str = self.input_adapter.dump_json((args, kwargs)).decode()
        workflow = workflows.get_or_create(self.name, input_str)
        self.currents.set(self.currents.get() + (workflow.id,))

        try:
            return self.func(*args, **kwargs.model_dump())
        except Exception:
            exc = True
            raise
        finally:
            assert self.currents.get()[-1] == workflow.id
            self.currents.set(self.currents.get()[:-1])
            if exc:
                workflows.failed(workflow)
            else:
                workflows.complete(workflow)

    @contextmanager
    def use(self, *args, **kwargs):
        bound = self.sig.bind(*args, **kwargs)
        args, kwargs = bound.args, bound.kwargs
        kwargs = self.kwarg_types(**kwargs)
        input_str = self.input_adapter.dump_json((args, kwargs)).decode()
        workflow = workflows.get_or_create(self.name, input_str)
        try:
            yield
        finally:
            workflows.complete(workflow)


class activity:
    def __init__(self, func):
        self.func = func
        self.name = func.__qualname__
        self.sig = inspect.signature(func)
        self.arg_types, self.kwarg_types = param_types(func)
        self.input_adapter = pydantic.TypeAdapter(tuple[self.arg_types, self.kwarg_types])

    def __call__(self, *args, **kwargs):
        if not workflow.currents.get():
            raise ValueError('No current workflow')
        workflow_id = workflow.currents.get()[-1]
        exc = None

        bound = self.sig.bind(*args, **kwargs)
        args, kwargs = bound.args, bound.kwargs
        kwargs = self.kwarg_types(**kwargs)
        input_str = self.input_adapter.dump_json((args, kwargs)).decode()

        activity = activities.may_find_one(workflow_id, self.name, input_str)
        if activity is not None:
            import json
            return json.loads(activity.output)

        try:
            ret = self.func(*args, **kwargs.model_dump())
            return ret
        except Exception:
            exc = True
            raise
        finally:
            if not exc:
                activity = Activity(workflow_id=workflow_id, name=self.name, input=input_str, output='{}')
                activities.save(activity)

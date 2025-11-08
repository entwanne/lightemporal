import inspect

import pydantic

from .backend import DB
from .models import param_types, Workflow, WorkflowStatus
from .repos import WorkflowRepository


workflows = WorkflowRepository(DB)


class workflow:
    def __init__(self, func):
        self.func = func
        self.name = func.__qualname__
        self.sig = inspect.signature(func)
        #self.input_adapter = params_adapter(func)
        self.arg_types, self.kwarg_types = param_types(func)
        self.input_adapter = pydantic.TypeAdapter(tuple[self.arg_types, self.kwarg_types])

    def __call__(self, *args, **kwargs):
        exc = False
        try:
            bound = self.sig.bind(*args, **kwargs)
            args, kwargs = bound.args, bound.kwargs
            kwargs = self.kwarg_types(**kwargs)
            #print(repr(kwargs), kwargs.model_dump())
            #print(b.args, b.kwargs)
            #user_list_adapter = pydantic.TypeAdapter(tuple[tuple[str]])
            input_str = self.input_adapter.dump_json((args, kwargs)).decode()
            workflow = workflows.get_or_create(self.name, input_str)
            print(workflow)
            return self.func(*args, **kwargs.model_dump())
        except Exception:
            exc = True
            raise
        finally:
            if not exc:
                workflows.complete(workflow)


class activity:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

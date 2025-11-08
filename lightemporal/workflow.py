import inspect

import pydantic

from .backend import DB
from .models import param_types, Workflow, WorkflowStatus


class workflow:
    def __init__(self, func):
        self.func = func
        self.name = func.__qualname__
        self.sig = inspect.signature(func)
        #self.input_adapter = params_adapter(func)
        self.arg_types, self.kwarg_types = param_types(func)
        self.input_adapter = pydantic.TypeAdapter(tuple[self.arg_types, self.kwarg_types])

    def __call__(self, *args, **kwargs):
        try:
            bound = self.sig.bind(*args, **kwargs)
            args, kwargs = bound.args, bound.kwargs
            kwargs = self.kwarg_types(**kwargs)
            #print(repr(kwargs), kwargs.model_dump())
            #print(b.args, b.kwargs)
            #user_list_adapter = pydantic.TypeAdapter(tuple[tuple[str]])
            input_str = self.input_adapter.dump_json((args, kwargs))
            #DB.cursor.execute('SELECT * FROM workflows WHERE name=? AND input=? AND status="FAILED"', (self.name, input_str))
            DB.cursor.execute('SELECT * FROM workflows WHERE name=? AND input=?', (self.name, input_str))
            #DB.cursor.execute('SELECT * FROM workflows')
            w = DB.cursor.fetchone()
            if w is None:
                w = Workflow(name=self.name, input=input_str, status=WorkflowStatus.RUNNING)
                DB.cursor.execute('INSERT INTO workflows(id, name, input, status) VALUES (?, ?, ?, ?)', (w.id, w.name, w.input, w.status.value))
                DB.db.commit()
            else:
                print('!!!', w)
                w_id, w_name, w_input, w_status = w
                w = Workflow(id=w_id, name=w_name, input=w_input, status=w_status)
                print(repr(w))
                pass
            return self.func(*args, **kwargs.model_dump())
        except Exception:
            raise
        else:
            pass


class activity:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

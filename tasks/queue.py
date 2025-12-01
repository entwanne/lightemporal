import heapq
import inspect
import time

import pydantic

from lightemporal.backend import DB
from lightemporal.models import param_types


class FuncQueue:
    def __init__(self, db, queue_id):
        self.db = db
        self.table = f'queue.{queue_id}'

    def put(self, func, /, *args, **kwargs):
        self.call_later(func, 0, *args, **kwargs)

    def call_later(self, func, duration, /, *args, **kwargs):
        self.call_at(func, time.time() + duration, *args, **kwargs)

    def call_at(self, func, timestamp, /, *args, **kwargs):
        sig = inspect.signature(func)
        arg_types, kwarg_types = param_types(func)
        bound = sig.bind(*args, **kwargs)
        args, kwargs = bound.args, kwarg_types(**bound.kwargs)
        adapter = pydantic.TypeAdapter(tuple[arg_types, kwarg_types])

        with self.db.atomic:
            heapq.heappush(
                self.db.db.setdefault(self.table, []),
                [timestamp, func.__name__, adapter.dump_python((args, kwargs), mode='json')],
            )

    def get(self, functions):
        while True:
            try:
                with DB.atomic:
                    queue = self.db.db[self.table]
                    if queue[0][0] <= time.time():
                        item = queue.pop(0)
                        break
            except (KeyError, IndexError):
                pass
            time.sleep(0.1)

        _, func_name, input_ = item
        func = functions[func_name]
        arg_types, kwarg_types = param_types(func)
        adapter = pydantic.TypeAdapter(tuple[arg_types, kwarg_types])
        args, kwargs = adapter.validate_python(input_)
        return func, args, kwargs.model_dump()


Q = FuncQueue(DB, 'tasks')

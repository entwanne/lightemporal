import heapq
import inspect
import time
import uuid

import pydantic

from ..core.context import ENV
from ..core.utils import repeat_if_needed, param_types


class FuncQueue:
    def __init__(self, db, queue_id):
        self.queue = db.queues[f'queue.{queue_id}']
        self.results = db.tables[f'results.{queue_id}']

    def put(self, func, /, *args, **kwargs):
        return self.call_later(func, 0, *args, **kwargs)

    def call_later(self, func, duration, /, *args, **kwargs):
        return self.call_at(func, time.time() + duration, *args, **kwargs)

    def call_at(self, func, timestamp, /, *args, **kwargs):
        return self._call(str(uuid.uuid4()), func, timestamp, 0, args, kwargs)

    def _call(self, task_id, func, timestamp, retry_count, args, kwargs):
        sig = inspect.signature(func)
        arg_types, kwarg_types = param_types(func)
        bound = sig.bind(*args, **kwargs)
        args, kwargs = bound.args, kwarg_types(**bound.kwargs)
        adapter = pydantic.TypeAdapter(tuple[arg_types, kwarg_types])
        self.queue.put([timestamp, task_id, func.__name__, retry_count, adapter.dump_python((args, kwargs), mode='json')])
        return task_id

    def get(self, functions):
        _, task_id, func_name, retry_count, input_ = self.queue.get_if(lambda item: item[0] <= time.time())
        func = functions[func_name]
        arg_types, kwarg_types = param_types(func)
        adapter = pydantic.TypeAdapter(tuple[arg_types, kwarg_types])
        args, kwargs = adapter.validate_python(input_)
        return task_id, func, retry_count, args, kwargs.model_dump()

    def get_result(self, task_id, func, blocking=True):
        sig = inspect.signature(func)
        adapter = pydantic.TypeAdapter(sig.return_annotation)

        for repeat_ctx in repeat_if_needed(exc_type=KeyError, blocking=blocking):
            with repeat_ctx, self.results.atomic:
                result = self.results.get(task_id)
                self.results.delete(task_id)
                if (error := result.get('error')) is not None:
                    raise ValueError(error)
                return adapter.validate_json(result['result'])

    def set_error(self, task_id, error_msg):
        self.results.set({'id': task_id, 'error': error_msg})

    def set_result(self, task_id, func, result):
        sig = inspect.signature(func)
        adapter = pydantic.TypeAdapter(sig.return_annotation)
        self.results.set({'id': task_id, 'result': adapter.dump_json(result).decode()})

    def execute(self, func, /, *args, **kwargs):
        task_id = self.put(func, *args, **kwargs)
        return self.get_result(task_id, func)


ENV['Q'] = FuncQueue(ENV['DB'], 'tasks')

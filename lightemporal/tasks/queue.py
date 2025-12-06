import heapq
import inspect
import time
import uuid

from ..core.context import ENV
from ..core.utils import repeat_if_needed, SignatureWrapper

from .discovery import get_task_name


class FuncQueue:
    def __init__(self, db, queue_id):
        self.queue = db.queues[f'queue.{queue_id}']
        self.suspended = db.tables[f'suspended.{queue_id}']
        self.results = db.tables[f'results.{queue_id}']

    def put(self, func, /, *args, **kwargs):
        return self.call_later(func, 0, *args, **kwargs)

    def call_later(self, func, duration, /, *args, **kwargs):
        return self.call_at(func, time.time() + duration, *args, **kwargs)

    def call_at(self, func, timestamp, /, *args, **kwargs):
        return self._call(str(uuid.uuid4()), func, timestamp, 0, args, kwargs)

    def _call(self, task_id, func, timestamp, retry_count, args, kwargs):
        sig = SignatureWrapper.from_function(func)
        self.queue.put([timestamp, task_id, get_task_name(func), retry_count, sig.dump_input(*args, **kwargs)])
        return task_id

    def _postpone(self, task_id, func, retry_count, args, kwargs):
        sig = SignatureWrapper.from_function(func)
        self.suspended.set({
            'id': task_id,
            'task': get_task_name(func),
            'retry_count': retry_count,
            'input': sig.dump_input(*args, **kwargs),
        })

    def _wakeup(self, task_id):
        with self.suspended.atomic:
            data = self.suspended.get(task_id)
            self.suspended.delete(task_id)
        self.queue.put([time.time(), task_id, data['task'], data['retry_count'], data['input']])

    def get(self, functions):
        _, task_id, func_name, retry_count, input_ = self.queue.get_if(lambda item: item[0] <= time.time())
        func = functions[func_name]
        sig = SignatureWrapper.from_function(func)
        args, kwargs = sig.load_input(input_)
        return task_id, func, retry_count, args, kwargs

    def get_result(self, task_id, func, blocking=True):
        sig = SignatureWrapper.from_function(func)

        for repeat_ctx in repeat_if_needed(exc_type=KeyError, blocking=blocking):
            with repeat_ctx, self.results.atomic:
                result = self.results.get(task_id)
                self.results.delete(task_id)
                if (error := result.get('error')) is not None:
                    raise ValueError(error)
                return sig.load_output(result['result'])

    def set_error(self, task_id, error_msg):
        self.results.set({'id': task_id, 'error': error_msg})

    def set_result(self, task_id, func, result):
        sig = SignatureWrapper.from_function(func)
        self.results.set({'id': task_id, 'result': sig.dump_output(result)})

    def execute(self, func, /, *args, **kwargs):
        task_id = self.put(func, *args, **kwargs)
        return self.get_result(task_id, func)


ENV['Q'] = FuncQueue(ENV['DB'], 'tasks')

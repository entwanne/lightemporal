import heapq
import inspect
import time
from collections.abc import Callable
from functools import cached_property

import pydantic

from ..core.context import ENV
from ..core.utils import repeat_if_needed, SignatureWrapper, UUID

from .discovery import get_task_name


class Task(pydantic.BaseModel):
    id: UUID
    name: str
    timestamp: float
    retry_count: int
    input: str


class TaskResult(pydantic.BaseModel):
    id: UUID
    result: str | None = None
    error: str | None = None


class TaskRepository:
    def __init__(self, db, queue_id):
        self.queue = db.queues[f'queue.{queue_id}']
        self.suspended = db.tables[f'suspended.{queue_id}']
        self.results = db.tables[f'results.{queue_id}']

    def add(self, task):
        row = task.model_dump(mode='json')
        timestamp = row.pop('timestamp')
        self.queue.put([timestamp, row])

    def suspend(self, task):
        self.suspended.set(task.model_dump(mode='json'))

    def wakeup(self, task_id):
        with self.suspended.atomic:
            try:
                data = self.suspended.get(task_id)
            except KeyError:
                return
            self.suspended.delete(task_id)
        self.add(Task.model_validate(data))

    def get_next_task(self):
        timestamp, row = self.queue.get_if(lambda item: item[0] <= time.time())
        return Task.model_validate({**row, 'timestamp': timestamp})

    def get_result(self, task_id, blocking=True):
        for repeat_ctx in repeat_if_needed(exc_type=KeyError, blocking=blocking):
            with repeat_ctx, self.results.atomic:
                result = self.results.get(task_id)
                self.results.delete(task_id)
                return TaskResult.model_validate(result)

    def set_result(self, task_result):
        self.results.set(task_result.model_dump(mode='json'))


class TaskFunction(pydantic.BaseModel):
    id: UUID
    func: Callable
    args: tuple
    kwargs: dict
    timestamp: float = pydantic.Field(default_factory=time.time)
    retry_count: int = 0

    @cached_property
    def name(self):
        return get_task_name(self.func)

    @cached_property
    def sig(self):
        return SignatureWrapper.from_function(self.func) 

    def to_task(self):
        return Task(
            id=self.id,
            name=self.name,
            input=self.sig.dump_input(*self.args, **self.kwargs),
            timestamp=self.timestamp,
            retry_count=self.retry_count
        )

    @classmethod
    def from_task(cls, func, task):
        sig = SignatureWrapper.from_function(func)
        assert get_task_name(func) == task.name
        args, kwargs = sig.load_input(task.input)
        taskf = cls(
            id=task.id,
            func=func,
            args=args,
            kwargs=kwargs,
            timestamp=task.timestamp,
            retry_count=task.retry_count,
        )
        taskf.name = task.name
        taskf.sig = sig
        return taskf

    def later(self, timestamp=None, duration=None):
        return self.model_copy(update={
            'timestamp': (time.time() if timestamp is None else timestamp) + (duration or 0)
        })

    def retry(self, delay=None):
        return self.model_copy(update={
            'retry_count': self.retry_count + 1,
            'timestamp': time.time() + (delay or 0),
        })


class FuncQueue:
    def __init__(self, db, queue_id):
        self.repo = TaskRepository(db, queue_id)

    def put(self, task):
        self.repo.add(task.to_task())
        return task

    def call(self, func, /, *args, **kwargs):
        return self.put(TaskFunction(func=func, args=args, kwargs=kwargs))

    def call_later(self, func, duration, /, *args, **kwargs):
        return self.put(TaskFunction(func=func, args=args, kwargs=kwargs, duration=duration))

    def call_at(self, func, timestamp, /, *args, **kwargs):
        return self.put(TaskFunction(func=func, args=args, kwargs=kwargs, timestamp=timestamp))

    def suspend(self, task):
        self.repo.suspend(task.to_task())

    def wakeup(self, task_id):
        self.repo.wakeup(task_id)

    def get(self, functions):
        task = self.repo.get_next_task()
        func = functions[task.name]
        return TaskFunction.from_task(func, task)

    def get_result(self, func, task_id, blocking=True):
        result = self.repo.get_result(task_id, blocking=blocking)

        if result.error is not None:
            raise ValueError(result.error)

        sig = SignatureWrapper.from_function(func)
        return sig.load_output(result.result)

    def set_result(self, task, result):
        self.repo.set_result(TaskResult(id=task.id, result=task.sig.dump_output(result)))

    def set_error(self, task, error_msg):
        self.repo.set_result(TaskResult(id=task.id, error=error_msg))

    def execute(self, func, /, *args, **kwargs):
        task_id = self.call(func, *args, **kwargs).id
        return self.get_result(func, task_id)


ENV['Q'] = FuncQueue(ENV['DB'], 'tasks')

import enum
import heapq
import inspect
import time
from collections.abc import Callable
from functools import cached_property

import pydantic

from ..core.context import ENV
from ..core.utils import repeat_if_needed, SignatureWrapper, UUID

from .discovery import get_task_name


class TaskStatus(enum.Enum):
    SCHEDULED = 'SCHEDULED'
    RUNNING = 'RUNNING'
    SUSPENDED = 'SUSPENDED'
    COMPLETED = 'COMPLETED'


class Task(pydantic.BaseModel):
    id: UUID
    name: str
    timestamp: float
    retry_count: int
    input: str
    queue_id: str
    status: TaskStatus


class TaskResult(pydantic.BaseModel):
    id: UUID
    result: str | None = None
    error: str | None = None


class TaskRepository:
    def __init__(self, db, queue_id):
        self.db = db
        # + handle queue id
        db.declare_table('tasks', Task)
        db.declare_table('results', TaskResult)

    def add(self, task):
        self.db.execute(
            """
            INSERT INTO tasks
            VALUES (:id, :name, :timestamp, :retry_count, :input, :queue_id, 'SCHEDULED')
            ON CONFLICT (id) DO UPDATE
            SET timestamp = :timestamp, retry_count = :retry_count, status = 'SCHEDULED'
            """,
            task,
            commit=True,
        )

    def suspend(self, task):
        self.db.execute(
            "UPDATE tasks SET status = 'SUSPENDED' WHERE id = ? AND status IN ('SCHEDULED', 'RUNNING')",
            (task.id,),
            commit=True,
        )

    def wakeup(self, task_id):
        self.db.execute(
            "UPDATE tasks SET status = 'SCHEDULED' WHERE id = ? AND status = 'SUSPENDED'",
            (task_id,),
            commit=True,
        )

    def get_next_task(self):
        for repeat_ctx in repeat_if_needed():
            with repeat_ctx:
                return self.db.query_one(
                    """
                    UPDATE tasks
                    SET status = 'RUNNING'
                    WHERE id = (
                        SELECT id
                        FROM tasks
                        WHERE status = 'SCHEDULED'
                        AND timestamp < ?
                    )
                    RETURNING *
                    """,
                    (time.time(),),
                    commit=True,
                    model=Task,
                )

    def get_result(self, task_id, blocking=True):
        for repeat_ctx in repeat_if_needed(exc_type=ValueError, blocking=blocking):
            with repeat_ctx, self.db.cursor(commit=True):
                self.db.execute(
                    "DELETE FROM tasks WHERE id = ?",
                    (task_id,),
                )
                return self.db.query_one(
                    "DELETE FROM results WHERE id = ? RETURNING *",
                    (task_id,),
                    model=TaskResult,
                )

    def set_result(self, task_result):
        with self.db.cursor(commit=True):
            self.db.execute(
                "UPDATE tasks SET status = 'COMPLETED' WHERE id = :id",
                task_result,
            )
            self.db.execute(
                """
                INSERT INTO results
                VALUES (:id, :result, :error)
                ON CONFLICT (id) DO UPDATE
                SET result = :result, error = :error
                """,
                task_result,
            )


class TaskFunction(pydantic.BaseModel):
    id: UUID
    func: Callable
    args: tuple
    kwargs: dict
    timestamp: float = pydantic.Field(default_factory=time.time)
    retry_count: int = 0
    queue_id: str = ''
    status: TaskStatus = TaskStatus.SCHEDULED

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
            retry_count=self.retry_count,
            queue_id=self.queue_id,
            status=self.status,
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
            queue_id=task.queue_id,
            status=task.status,
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

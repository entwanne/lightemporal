import heapq
import json
import time
from contextlib import contextmanager
from functools import cache, cached_property
from pathlib import Path

from .context import ENV
from .lock import FileLock
from .utils import repeat_if_needed


class Backend:
    def __init__(self, path='lightemporal.db'):
        self.path = Path(path)
        self._lock = FileLock(self.path.with_name(self.path.name + '.lock'), reentrant=True)

        self._tables = None

    def reload(self):
        with self._lock:
            if not self.path.exists():
                if not self.path.exists():
                    self.path.write_text('{}')
            with self.path.open() as f:
                self._tables = json.load(f)

    def commit(self):
        with self._lock:
            with self.path.open('w') as f:
                json.dump(self._tables, f)

    @property
    @contextmanager
    def atomic(self):
        with self._lock:
            try:
                self.reload()
                yield
            finally:
                self.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._tables = None

    @cached_property
    def tables(self):
        return TableView(self)

    @cached_property
    def queues(self):
        return QueueView(self)


class TableView:
    def __init__(self, db):
        self.db = db

    @cache
    def __getitem__(self, name):
        return Table(self.db, name)


class QueueView:
    def __init__(self, db):
        self.db = db

    @cache
    def __getitem__(self, name):
        return Queue(self.db, name)


class _Table:
    def __init__(self, db, name):
        self.db = db
        self.name = name

    @property
    def reload(self):
        return self.db.reload

    @property
    def commit(self):
        return self.db.commit

    @property
    def atomic(self):
        return self.db.atomic


class Table(_Table):
    def get(self, id):
        self.db.reload()
        return self.db._tables.get(self.name, {})[id]

    def list(self, **filters):
        self.db.reload()
        for row in self.db._tables.get(self.name, {}).values():
            if all(row.get(key) == value for key, value in filters.items()):
                yield row

    def set(self, row):
        with self.db.atomic:
            self.db._tables.setdefault(self.name, {})[row['id']] = row

    def delete(self, id):
        with self.db.atomic:
            del self.db._tables.setdefault(self.name, {})[id]


class Queue(_Table):
    def get_if(self, condition, blocking=True):
        for repeat_ctx in repeat_if_needed(
                exc_type=IndexError,
                blocking=blocking,
                error=ValueError('Queue is empty'),
        ):
            with repeat_ctx, self.db.atomic:
                queue = self.db._tables.setdefault(self.name, [])
                if condition(queue[0]):
                    return queue.pop(0)

    def get(self, blocking=True):
        return self.get_if(lambda item: True, blocking=blocking)

    def put(self, value):
        with self.db.atomic:
            heapq.heappush(
                self.db._tables.setdefault(self.name, []),
                value,
            )


ENV.add_context('DB', Backend())

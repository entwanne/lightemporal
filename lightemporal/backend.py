import atexit
import heapq
import json
import time
from contextlib import contextmanager
from functools import cache, cached_property
from pathlib import Path

from .lock import FileLock


class Backend:
    def __init__(self, path='lightemporal.db'):
        self.path = Path(path)
        self._write_lock = FileLock(self.path.with_name(self.path.name + '.lock'), reentrant=True)

        self._tables = None

    def reload(self):
        if not self.path.exists():
            with self._write_lock:
                if not self.path.exists():
                    self.path.write_text('{}')
        with self.path.open() as f:
            self._tables = json.load(f)

    def commit(self):
        with self._write_lock:
            with self.path.open('w') as f:
                json.dump(self._tables, f)

    @property
    @contextmanager
    def atomic(self):
        with self._write_lock:
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


class Queue(_Table):
    def get_if(self, condition, blocking=True):
        while True:
            try:
                with self.db.atomic:
                    queue = self.db._tables.setdefault(self.name, [])
                    if condition(queue[0]):
                        return queue.pop(0)
            except IndexError:
                pass

            if blocking:
                time.sleep(0.1)
            else:
                raise ValueError('Queue is empty')

    def get(self, blocking=True):
        return self.get_if(lambda item: True, blocking=blocking)

    def put(self, value):
        with self.db.atomic:
            heapq.heappush(
                self.db._tables.setdefault(self.name, []),
                value,
            )


backend_ctx = Backend()
DB = backend_ctx.__enter__()
atexit.register(backend_ctx.__exit__, None, None, None)

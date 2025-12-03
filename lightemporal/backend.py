import atexit
import json
from contextlib import contextmanager
from functools import cache, cached_property
from pathlib import Path

from .lock import FileLock


class Backend:
    def __init__(self, path='lightemporal.db'):
        self.path = Path(path)
        self._write_lock = FileLock(self.path.with_name(self.path.name + '.lock'))

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


class TableView:
    def __init__(self, db):
        self.db = db

    @cache
    def __getitem__(self, name):
        return Table(self.db, name)


class Table:
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


backend_ctx = Backend()
DB = backend_ctx.__enter__()
atexit.register(backend_ctx.__exit__, None, None, None)

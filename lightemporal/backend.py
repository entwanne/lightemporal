import atexit
import json
from contextlib import contextmanager
from pathlib import Path

from .lock import FileLock


class Backend:
    def __init__(self, db_path='lightemporal.db'):
        self.db_path = Path(db_path)
        self.write_lock = FileLock(self.db_path.with_name(self.db_path.name + '.lock'))

        self.db = None

    def reload(self):
        if not self.db_path.exists():
            with self.write_lock:
                if not self.db_path.exists():
                    self.db_path.write_text('{}')
        with self.db_path.open() as f:
            self.db = json.load(f)

    def commit(self):
        with self.write_lock:
            with self.db_path.open('w') as f:
                json.dump(self.db, f)

    @property
    @contextmanager
    def atomic(self):
        with self.write_lock:
            try:
                self.reload()
                yield
            finally:
                self.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.db = None

    def get(self, table, id):
        self.reload()
        return self.db.get(table, {})[id]

    def list(self, table, **filters):
        self.reload()
        for row in self.db.get(table, {}).values():
            if all(row.get(key) == value for key, value in filters.items()):
                yield row

    def set(self, table, row):
        with self.atomic:
            self.db.setdefault(table, {})[row['id']] = row


backend_ctx = Backend()
DB = backend_ctx.__enter__()
atexit.register(backend_ctx.__exit__, None, None, None)

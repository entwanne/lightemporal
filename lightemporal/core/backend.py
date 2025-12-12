import contextvars
import sqlite3
import time
from contextlib import contextmanager
from functools import cache, cached_property
from pathlib import Path

import pydantic

from .context import ENV


def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


class Backend:
    def __init__(self, path='lightemporal.db'):
        self.path = Path(path)
        self.connection = None
        self._cursor = contextvars.ContextVar('_cursor', default=None)

    @contextmanager
    def cursor(self, commit=False):
        cursor = self._cursor.get()
        if cursor is not None:
            yield cursor
            return

        has_error = False

        try:
            cursor = self.connection.cursor()
            self._cursor.set(cursor)
            yield cursor
        except Exception:
            has_error = True
            raise
        finally:
            if has_error:
                self.connection.rollback()
            elif commit:
                self.connection.commit()
            try:
                cursor.close()
            except sqlite3.ProgrammingError:
                pass

            self._cursor.set(None)

    def __enter__(self):
        self.connection = sqlite3.connect(self.path, autocommit=False)
        self.connection.row_factory = dict_factory
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.connection.close()

    def declare_table(self, name, model_cls, *indexes):
        with self.cursor(commit=True) as cursor:
            struct = ', '.join(model_cls.model_fields)
            cursor.execute(f'CREATE TABLE IF NOT EXISTS {name} ({struct})')
            cursor.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS ux_{name}_id ON {name}(id)')
            for index in indexes:
                cursor.execute(index)

    def execute(self, req, data=(), commit=False):
        with self.cursor(commit=commit) as cursor:
            if isinstance(data, pydantic.BaseModel):
                data = data.model_dump(mode='json')

            cursor.execute(req, data)

    def query_one(self, req, *args, model=None, commit=False):
        with self.cursor(commit=commit) as cursor:
            self.execute(req, *args)
            row = cursor.fetchone()
            if row is None:
                raise ValueError

        if model is not None:
            row = model.model_validate(row)
        return row

    def query(self, req, *args, model=None, commit=False):
        with self.cursor(commit=commit) as cursor:
            self.execute(req, *args)
            while True:
                row = cursor.fetchone()
                if row is None:
                    break

                if model is not None:
                    row = model.model_validate(row)
                yield row


ENV.add_context('DB', Backend())

import sqlite3
import types

import pydantic
import pytest

from lightemporal.core.backend import Backend


class Model(pydantic.BaseModel):
    id: int
    name: str


@pytest.fixture
def db():
    with Backend(':memory:') as database:
        database.execute('CREATE TABLE ids (id)', commit=True)
        yield database


def test_query_one(db):
    # One row
    assert db.query_one('SELECT 1 AS value') == {'value': 1}
    # Multiple rows
    assert db.query_one('SELECT 1 AS value UNION SELECT 2 AS value') == {'value': 1}
    # No row
    with pytest.raises(ValueError):
        db.query_one('SELECT 1 AS value WHERE value > 1')

    # Model
    assert db.query_one("SELECT 1 AS id, 'test' AS name", model=Model) == Model(id=1, name='test')

    # Arguments
    assert db.query_one('SELECT ? + ? AS value', (3, 5)) == {'value': 8}
    assert db.query_one('SELECT :x + :y AS value', {'x': 3, 'y': 5}) == {'value': 8}

    assert db.query_one("SELECT :id + 1 AS id, concat(:name, '_') AS name", Model(id=1, name='test')) == {'id': 2, 'name': 'test_'}
    assert db.query_one("SELECT :id + 1 AS id, concat(:name, '_') AS name", Model(id=1, name='test'), model=Model) == Model(id=2, name='test_')

    # Without commit
    assert db.query_one('INSERT INTO ids VALUES (?) RETURNING id', (1,)) == {'id': 1}
    assert db.query_one('SELECT * FROM ids') == {'id': 1}
    db.connection.rollback()
    with pytest.raises(ValueError):
        db.query_one('SELECT * FROM ids')

    # With commit
    assert db.query_one('INSERT INTO ids VALUES (?) RETURNING id', (1,), commit=True) == {'id': 1}
    assert db.query_one('SELECT * FROM ids') == {'id': 1}
    db.connection.rollback()
    assert db.query_one('SELECT * FROM ids') == {'id': 1}


def test_query(db):
    # Return type
    ret = db.query('SELECT 1 AS value')
    assert isinstance(ret, types.GeneratorType)
    assert iter(ret) is ret
    assert next(ret) == {'value': 1}
    with pytest.raises(StopIteration):
        next(ret)

    # One row
    assert list(db.query('SELECT 1 AS value')) == [{'value': 1}]
    # Multiple rows
    assert list(db.query('SELECT 1 AS value UNION SELECT 2 AS value')) == [{'value': 1}, {'value': 2}]
    # No row
    assert list(db.query('SELECT 1 AS value WHERE value > 1')) == []

    # Model
    assert list(db.query("SELECT 1 AS id, 'test' AS name", model=Model)) == [Model(id=1, name='test')]

    # Arguments
    assert list(db.query('SELECT ? + ? AS value', (3, 5))) == [{'value': 8}]
    assert list(db.query('SELECT :x + :y AS value', {'x': 3, 'y': 5})) == [{'value': 8}]

    assert list(db.query("SELECT :id + 1 AS id, concat(:name, '_') AS name", Model(id=1, name='test'))) == [{'id': 2, 'name': 'test_'}]
    assert list(db.query("SELECT :id + 1 AS id, concat(:name, '_') AS name", Model(id=1, name='test'), model=Model)) == [Model(id=2, name='test_')]

    # Without commit
    assert list(db.query('INSERT INTO ids VALUES (?) RETURNING id', (1,))) == [{'id': 1}]
    assert list(db.query('SELECT * FROM ids')) == [{'id': 1}]
    db.connection.rollback()
    assert list(db.query('SELECT * FROM ids')) == []

    # With commit
    assert list(db.query('INSERT INTO ids VALUES (?) RETURNING id', (1,), commit=True)) == [{'id': 1}]
    assert list(db.query('SELECT * FROM ids')) == [{'id': 1}]
    db.connection.rollback()
    assert list(db.query('SELECT * FROM ids')) == [{'id': 1}]

    # Incomplete generator
    gen = db.query('SELECT 1 AS value UNION SELECT 2 AS value')
    assert next(gen) == {'value': 1}
    # with other call before it's finished
    assert list(db.query('SELECT 3 AS value')) == [{'value': 3}]
    # then generator is exhausted
    with pytest.raises(StopIteration):
        next(gen)


def test_execute(db):
    # No return value
    assert db.execute('SELECT 1 AS value') is None
    assert db.execute('SELECT 1 AS value UNION SELECT 2 AS value') is None
    assert db.execute('SELECT 1 AS value WHERE value > 1') is None

    # Arguments (without commit)
    db.execute('INSERT INTO ids VALUES (1)')
    db.execute('INSERT INTO ids VALUES (?)', (2,))
    db.execute('INSERT INTO ids VALUES (:id)', {'id': 3})
    db.execute('INSERT INTO ids VALUES (:id)', Model(id=4, name=''))

    assert {row['id'] for row in db.query('SELECT * FROM ids')} == {1, 2, 3, 4}

    db.connection.rollback()
    assert {row['id'] for row in db.query('SELECT * FROM ids')} == set()

    # With commit
    db.execute('INSERT INTO ids VALUES (1)', commit=True)
    db.execute('INSERT INTO ids VALUES (?)', (2,), commit=True)
    db.execute('INSERT INTO ids VALUES (:id)', {'id': 3}, commit=True)
    db.execute('INSERT INTO ids VALUES (:id)', Model(id=4, name=''), commit=True)

    assert {row['id'] for row in db.query('SELECT * FROM ids')} == {1, 2, 3, 4}

    db.connection.rollback()
    assert {row['id'] for row in db.query('SELECT * FROM ids')} == {1, 2, 3, 4}


def test_context_manager():
    db = Backend(':memory:')

    # Before entering context

    with pytest.raises(Exception):
        db.query_one('SELECT 1 AS val')

    # Within context

    with db:
        result = db.query_one('SELECT 1 AS val')

    assert result == {'val': 1}

    # After leaving context

    with pytest.raises(Exception):
        db.query_one('SELECT 1 AS val')


def test_cursor(db):
    # Cursor without commit
    with db.cursor() as cursor:
        db.execute('INSERT INTO ids VALUES (?)', (1,), commit=False)
        db.execute('INSERT INTO ids VALUES (?)', (2,), commit=True)
        results = {row['id'] for row in db.query('SELECT * FROM ids')}

    assert results == {1, 2}
    assert {row['id'] for row in db.query('SELECT * FROM ids')} == {1, 2}

    db.connection.rollback()
    assert {row['id'] for row in db.query('SELECT * FROM ids')} == set()

    # Cursor is closed
    with pytest.raises(sqlite3.ProgrammingError):
        cursor.fetchone()

    # Cursor with commit
    with db.cursor(commit=True) as cursor:
        db.execute('INSERT INTO ids VALUES (?)', (1,), commit=False)
        db.execute('INSERT INTO ids VALUES (?)', (2,), commit=True)
        results = {row['id'] for row in db.query('SELECT * FROM ids')}

    assert results == {1, 2}
    assert {row['id'] for row in db.query('SELECT * FROM ids')} == {1, 2}

    db.connection.rollback()
    assert {row['id'] for row in db.query('SELECT * FROM ids')} == {1, 2}

    # Cursor is closed
    with pytest.raises(sqlite3.ProgrammingError):
        cursor.fetchone()

    # Rollback & no commit on error
    with pytest.raises(ValueError), db.cursor(commit=True) as cursor:
        db.execute('INSERT INTO ids VALUES (?)', (3,))
        results = {row['id'] for row in db.query('SELECT * FROM ids')}
        raise ValueError

    assert results == {1, 2, 3}
    assert {row['id'] for row in db.query('SELECT * FROM ids')} == {1, 2}


def test_declare_table(db):
    db.declare_table('models', Model)
    assert {row['sql'] for row in db.query("SELECT * FROM sqlite_schema WHERE tbl_name = 'models'")} == {
        'CREATE TABLE models (id, name)',
        'CREATE UNIQUE INDEX ux_models_id ON models(id)',
    }

    db.declare_table(
        'models2',
        Model,
        'CREATE INDEX ix_models2_name ON models2(name)',
        'CREATE INDEX ix_models2_id_name ON models2(id, name)',
    )
    assert {row['sql'] for row in db.query("SELECT * FROM sqlite_schema WHERE tbl_name = 'models2'")} == {
        'CREATE TABLE models2 (id, name)',
        'CREATE UNIQUE INDEX ux_models2_id ON models2(id)',
        'CREATE INDEX ix_models2_name ON models2(name)',
        'CREATE INDEX ix_models2_id_name ON models2(id, name)',
    }

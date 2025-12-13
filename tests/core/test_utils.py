import time
import uuid

import pydantic
import pytest

from lightemporal.core.utils import UUID, repeat_if_needed, SignatureWrapper


def test_uuid():
    class Model(pydantic.BaseModel):
        id: UUID

    m1 = Model()
    m2 = Model()
    assert str(uuid.UUID(m1.id)) == m1.id
    assert str(uuid.UUID(m2.id)) == m2.id
    assert m1.id != m2.id


def test_repeat_if_needed(monkeypatch):
    logs = []
    monkeypatch.setattr(time, 'sleep', lambda d: logs.append(('sleep', d)))

    # Default parameters

    for i, ctx in enumerate(repeat_if_needed()):
        with ctx:
            logs.append('inside_block')
            if i < 2:
                logs.append('pass')
                pass
            else:
                logs.append('break')
                break

    assert logs == [
        'inside_block',
        'pass',
        ('sleep', 0.1),
        'inside_block',
        'pass',
        ('sleep', 0.1),
        'inside_block',
        'break',
    ]
    logs.clear()

    for i, ctx in enumerate(repeat_if_needed()):
        with ctx:
            logs.append('inside_block')
            if i < 2:
                logs.append('raise')
                raise ValueError
            else:
                logs.append('break')
                break

    assert logs == [
        'inside_block',
        'raise',
        ('sleep', 0.1),
        'inside_block',
        'raise',
        ('sleep', 0.1),
        'inside_block',
        'break',
    ]
    logs.clear()

    # Sleep time

    for i, ctx in enumerate(repeat_if_needed(sleep_time=1)):
        with ctx:
            logs.append('inside_block')
            if i < 2:
                logs.append('raise')
                raise ValueError
            else:
                logs.append('break')
                break

    assert logs == [
        'inside_block',
        'raise',
        ('sleep', 1),
        'inside_block',
        'raise',
        ('sleep', 1),
        'inside_block',
        'break',
    ]
    logs.clear()

    # Non blocking

    for ctx in repeat_if_needed(blocking=False):
        with ctx:
            logs.append('inside_block')
            break

    assert logs == [
        'inside_block',
    ]
    logs.clear()

    with pytest.raises(RuntimeError):
        for ctx in repeat_if_needed(blocking=False):
            with ctx:
                logs.append('inside_block')

    assert logs == [
        'inside_block',
    ]
    logs.clear()

    with pytest.raises(ValueError) as err:
        for ctx in repeat_if_needed(blocking=False):
            with ctx:
                logs.append('inside_block')
                raise ValueError('foo')

    assert logs == [
        'inside_block',
    ]
    logs.clear()

    assert err.value.args == ('foo',)
    assert err.value.__cause__ is None

    with pytest.raises(ValueError) as err:
        for ctx in repeat_if_needed(blocking=False, error=ValueError('bar')):
            with ctx:
                logs.append('inside_block')

    assert logs == [
        'inside_block',
    ]
    logs.clear()

    assert err.value.args == ('bar',)
    assert err.value.__cause__ is None

    with pytest.raises(ValueError) as err:
        for ctx in repeat_if_needed(blocking=False, error=ValueError('bar')):
            with ctx:
                logs.append('inside_block')
                raise ValueError('foo')

    assert logs == [
        'inside_block',
    ]
    logs.clear()

    assert err.value.args == ('bar',)
    assert isinstance(err.value.__cause__, ValueError)
    assert err.value.__cause__.args == ('foo',)

    # Specific exceptions to catch

    with pytest.raises(ValueError) as err:
        for i, ctx in enumerate(repeat_if_needed(exc_type=TypeError)):
            with ctx:
                logs.append('inside_block')
                if i < 2:
                    logs.append('raise')
                    raise ValueError
                else:
                    logs.append('break')
                    break

    assert logs == ['inside_block', 'raise']
    logs.clear()

    for i, ctx in enumerate(repeat_if_needed(exc_type=TypeError)):
        with ctx:
            logs.append('inside_block')
            if i < 2:
                logs.append('raise')
                raise TypeError
            else:
                logs.append('break')
                break

    assert logs == [
        'inside_block',
        'raise',
        ('sleep', 0.1),
        'inside_block',
        'raise',
        ('sleep', 0.1),
        'inside_block',
        'break',
    ]
    logs.clear()


def test_signature_wrapper():
    assert False

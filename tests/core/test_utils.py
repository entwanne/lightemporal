import inspect
import time
import types
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
    def func(pos_only1: int, pos_only2: str, /, pos_or_kwarg1: str, pos_or_kwarg2: list[int] = [], *, kw_only1: tuple[float, str], kw_only2: dict[str, str] = {}) -> tuple[str, list[int]]:
        return (
            ':'.join((pos_only2, pos_or_kwarg1, kw_only1[1], *(f'{k}={v}' for k, v in kw_only2.items()))),
            [pos_only1, *pos_or_kwarg2, int(kw_only1[0])],
        )

    # Construction

    sig = SignatureWrapper.from_function(func)
    assert vars(sig) == {'signature': inspect.signature(func)}

    assert vars(SignatureWrapper(inspect.signature(func))) == vars(sig)
    assert vars(SignatureWrapper(inspect.signature(test_signature_wrapper))) != vars(sig)

    # Args model

    model = sig.args_model
    assert isinstance(model, types.GenericAlias)
    assert model.__origin__ is tuple
    assert model.__args__ == (int, str, str, list[int])

    # Kwargs model

    model = sig.kwargs_model
    assert issubclass(model, pydantic.BaseModel)
    assert model.model_fields.keys() == {'kw_only1', 'kw_only2'}

    instance = model(kw_only1=('1', 'two'), kw_only2={'three': 'four'})
    assert instance.kw_only1 == (1.0, 'two')
    assert instance.kw_only2 == {'three': 'four'}

    # Input adapter

    value = sig.input_adapter.validate_python((('1', 'two', 'three', (4, '5')), {'kw_only1': ('6', 'seven'), 'kw_only2': {'eight': 'nine'}}))
    assert value == (
        (1, 'two', 'three', [4, 5]),
        sig.kwargs_model(kw_only1=(6.0, 'seven'), kw_only2={'eight': 'nine'}),
    )

    # Output adapter

    assert sig.output_adapter.validate_python(['one', (2, 3, 4.0)]) == ('one', [2, 3, 4])

    # Dump input

    assert sig.dump_input(1, 'two', 'three', pos_or_kwarg2=[4, 5, 6], kw_only1=(7.8, 'nine'), kw_only2={'ten': 'eleven'}) == '[[1,"two","three",[4,5,6]],{"kw_only1":[7.8,"nine"],"kw_only2":{"ten":"eleven"}}]'

    # Dump input with default values

    assert sig.dump_input(1, 'two', 'three', kw_only1=(4.5, 'six')) == '[[1,"two","three",[]],{"kw_only1":[4.5,"six"],"kw_only2":{}}]'

    # Load input

    assert sig.load_input('[[1,"two","three",[4,5,6]],{"kw_only1":[7.8,"nine"],"kw_only2":{"ten":"eleven"}}]') == (
        (1, 'two', 'three', [4, 5, 6]),
        {
            'kw_only1': (7.8, 'nine'),
            'kw_only2': {'ten': 'eleven'},
        },
    )

    # Dump output

    assert sig.dump_output(('one', [2, 3])) == '["one",[2,3]]'

    # Load output

    assert sig.load_output('["one",[2,3]]') == ('one', [2, 3])

    # Integration

    input_str = sig.dump_input(1, 'two', 'three', pos_or_kwarg2=[4, 5, 6], kw_only1=(7.8, 'nine'), kw_only2={'ten': 'eleven'})
    args, kwargs = sig.load_input(input_str)
    ret = func(*args, **kwargs)
    output_str = sig.dump_output(ret)
    assert sig.load_output(output_str) == ret == (
        'two:three:nine:ten=eleven',
        [1, 4, 5, 6, 7],
    )

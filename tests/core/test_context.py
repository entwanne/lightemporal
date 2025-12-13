import pytest

from lightemporal.core.context import Context, enter_global_manager, ENV


def test_context():
    ctx = Context()

    # Empty context

    assert ctx.keys() == set()
    assert list(ctx) == []
    assert dict(ctx) == {}
    assert ctx.depth == 0

    with pytest.raises(KeyError):
        ctx['foo']

    with pytest.raises(TypeError):
        ctx['foo'] = object()

    # Layer 1

    layer1 = ctx.new_layer()
    assert layer1.__enter__() is ctx

    ctx['foo'] = base_foo = object()
    assert ctx['foo'] is base_foo

    ctx['bar'] = base_bar = object()
    assert ctx['bar'] is base_bar

    assert ctx.keys() == {'foo', 'bar'}
    assert list(ctx) == ['foo', 'bar']
    assert dict(ctx) == {'foo': base_foo, 'bar': base_bar}
    assert ctx.depth == 1

    # Layer 2

    layer2 = ctx.new_layer()
    assert layer2.__enter__() is ctx

    assert dict(ctx) == {'foo': base_foo, 'bar': base_bar}

    ctx['bar'] = new_bar = object()
    ctx['baz'] = new_baz = object()

    assert ctx['foo'] is base_foo
    assert ctx['bar'] is new_bar
    assert ctx['baz'] is new_baz

    assert dict(ctx) == {'foo': base_foo, 'bar': new_bar, 'baz': new_baz}
    assert ctx.depth == 2

    # Close layers

    layer2.__exit__(None, None, None)
    assert dict(ctx) == {'foo': base_foo, 'bar': base_bar}
    assert ctx.depth == 1

    layer1.__exit__(None, None, None)
    assert dict(ctx) == {}
    assert ctx.depth == 0

    # Layer as context manager

    with ctx.new_layer() as ctx2:
        ctx2['foo'] = 42
        result = dict(ctx2)

    assert ctx2 is ctx
    assert result == {'foo': 42}
    assert dict(ctx) == {}


def test_context_update():
    ctx = Context()

    with pytest.raises(TypeError):
        ctx.update({'foo': 0})

    with ctx.new_layer():
        ctx.update({'foo': 0, 'bar': 1})
        result1 = dict(ctx)

        with ctx.new_layer():
            ctx.update({'bar': 2, 'baz': 3})
            result2 = dict(ctx)

        ctx.update({'spam': 4})
        result3 = dict(ctx)

    assert result1 == {'foo': 0, 'bar': 1}
    assert result2 == {'foo': 0, 'bar': 2, 'baz': 3}
    assert result3 == {'foo': 0, 'bar': 1, 'spam': 4}
    assert dict(ctx) == {}


def test_context_add_context():
    class LogContext:
        logs = []

        def __init__(self, id, value, catch=False):
            self.id = id
            self.value = value
            self.catch = catch

        def __enter__(self):
            self.logs.append(('enter', self.id))
            return self.value

        def __exit__(self, exc_type, exc_value, exc_tb):
            self.logs.append(('exit', self.id))
            return self.catch

    ctx = Context()

    with ctx.new_layer():
        assert ctx.add_context('foo', LogContext('outer_foo', 0)) is None
        ctx.add_context('bar', LogContext('outer_bar', 1))

        LogContext.logs.append(dict(ctx))

        with ctx.new_layer():
            ctx.add_context('bar', LogContext('inner_bar', 2))
            ctx.add_context('baz', LogContext('inner_baz', 3))
            LogContext.logs.append(dict(ctx))

        LogContext.logs.append(dict(ctx))

    assert LogContext.logs == [
        ('enter', 'outer_foo'),
        ('enter', 'outer_bar'),
        {'foo': 0, 'bar': 1},
        ('enter', 'inner_bar'),
        ('enter', 'inner_baz'),
        {'foo': 0, 'bar': 2, 'baz': 3},
        ('exit', 'inner_baz'),
        ('exit', 'inner_bar'),
        {'foo': 0, 'bar': 1},
        ('exit', 'outer_bar'),
        ('exit', 'outer_foo'),
    ]

    # With exception

    LogContext.logs.clear()

    with pytest.raises(TypeError), ctx.new_layer():
        ctx.add_context('foo', LogContext('foo', 0))

        LogContext.logs.append(dict(ctx))

        with ctx.new_layer():
            ctx.add_context('bar', LogContext('first_bar', 1, catch=True))
            LogContext.logs.append(dict(ctx))
            raise TypeError

        ctx.add_context('baz', LogContext('first_baz', 2))
        LogContext.logs.append(dict(ctx))

        with ctx.new_layer():
            ctx.add_context('bar', LogContext('second_bar', 3, catch=False))
            LogContext.logs.append(dict(ctx))
            raise TypeError

        ctx.add_context('baz', LogContext('second_baz', 4))
        LogContext.logs.append(dict(ctx))

    assert LogContext.logs == [
        ('enter', 'foo'),
        {'foo': 0},
        ('enter', 'first_bar'),
        {'foo': 0, 'bar': 1},
        ('exit', 'first_bar'),
        ('enter', 'first_baz'),
        {'foo': 0, 'baz': 2},
        ('enter', 'second_bar'),
        {'foo': 0, 'bar': 3, 'baz': 2},
        ('exit', 'second_bar'),
        ('exit', 'first_baz'),
        ('exit', 'foo'),
    ]


def test_enter_global_manager(monkeypatch):
    import atexit

    class ContextManager:
        def __init__(self):
            self.entered = False
            self.exited = False

        def __enter__(self):
            self.entered = True
            return 'ok'

        def __exit__(self, exc_type, exc_value, exc_tb):
            self.exited = True

    atexit_calls = []
    monkeypatch.setattr(atexit, 'register', lambda func, *args: atexit_calls.append((func, args)))

    ctx = ContextManager()
    assert enter_global_manager(ctx) == 'ok'
    assert ctx.entered
    assert not ctx.exited

    assert atexit_calls == [(ctx.__exit__, (None, None, None))]
    for func, args in atexit_calls:
        func(*args)

    assert ctx.entered
    assert ctx.exited


def test_env():
    assert ENV.depth > 0

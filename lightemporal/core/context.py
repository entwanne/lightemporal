import atexit
from collections import ChainMap
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar


class Context:
    def __init__(self):
        self._map = ContextVar('_map', default=ChainMap(_BaseMap()))

    @contextmanager
    def new_layer(self):
        with _ContextLayer(self):
            yield self

    def add_context(self, name, ctx):
        return self._map.get().maps[0].add_context(name, ctx)

    def keys(self):
        return self._map.get().keys()

    def __iter__(self):
        yield from self.keys()

    def __getitem__(self, key):
        return self._map.get()[key]

    def __setitem__(self, key, value):
        self._map.get()[key] = value

    def __delitem__(self, key):
        del self._map.get()[key]

    def update(self, mapping):
        self._map.get().update(mapping)


class _BaseMap:
    def keys(self):
        return ()

    def __iter__(self):
        yield from ()

    def __getitem__(self, key):
        raise KeyError(key)


class _ContextLayer:
    def __init__(self, context):
        self.context = context
        self.stack = ExitStack()
        self.mapping = {}

    def __enter__(self):
        self.context._map.set(self.context._map.get().new_child(self))
        self.stack.__enter__()

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.stack.__exit__(exc_type, exc_value, exc_tb)
        self.context._map.set(self.context._map.get().parents)

    def add_context(self, name, ctx):
        self.mapping[name] = self.stack.enter_context(ctx)

    def keys(self):
        return self.mapping.keys()

    def __iter__(self):
        yield from self.keys()

    def __getitem__(self, key):
        return self.mapping[key]

    def __setitem__(self, key, value):
        self.mapping[key] = value

    def __delitem__(self, key):
        del self.mapping[key]

    def update(self, mapping):
        self.mapping.update(mapping)


def enter_global_manager(manager):
    ret = manager.__enter__()
    atexit.register(manager.__exit__, None, None, None)
    return ret


ENV = Context()
enter_global_manager(ENV.new_layer())

import inspect
import time
from contextlib import contextmanager

import pydantic


@contextmanager
def _repeat_context(loop):
    try:
        yield
    except loop.exc_type as e:
        if loop.error is None:
            loop.error = e


class repeat_if_needed:
    def __init__(self, *, exc_type=Exception, blocking=True, sleep_time=0.1, error=None):
        self.exc_type = exc_type
        self.blocking = blocking
        self.sleep_time = sleep_time
        self.error = error

    def __iter__(self):
        while True:
            yield _repeat_context(self)

            if self.blocking:
                time.sleep(self.sleep_time)
            else:
                raise self.error or RuntimeError()


def param_types(f):
    sig = inspect.signature(f)

    args = tuple[*(
        p.annotation
        for p in sig.parameters.values()
        if p.kind is not p.KEYWORD_ONLY
    )]
    kwargs = pydantic.create_model(
        'kwargs',
        **{
            p.name: p.annotation
            for p in sig.parameters.values()
            if p.kind is p.KEYWORD_ONLY
        }
    )
    return args, kwargs

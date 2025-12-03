import time
from contextlib import contextmanager


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

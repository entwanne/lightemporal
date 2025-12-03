import contextvars
import time
from pathlib import Path

from .utils import repeat_if_needed


class FileLock:
    def __init__(self, path, block=True, reentrant=False):
        self.path = Path(path)
        self.block = block
        self.reentrant = reentrant
        self._stack = contextvars.ContextVar('stack', default=())

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def acquire(self, block=None):
        if block is None:
            block = self.block

        stack = self._stack.get()

        if stack:
            if self.reentrant:
                self._stack.set((*stack, None))
                return
            else:
                raise ValueError('Deadlock')

        for repeat_ctx in repeat_if_needed(
                exc_type=FileExistsError,
                blocking=block,
                error=ValueError('Cannot acquire lock'),
        ):
            with repeat_ctx:
                stack = (self.path.open('x'),)
                break

        self._stack.set(stack)

    def release(self):
        stack = self._stack.get()

        if not stack:
            raise ValueError('No lock acquired')

        if stack[-1] is not None:
            self.path.unlink()
            stack[-1].close()

        self._stack.set(stack[:-1])

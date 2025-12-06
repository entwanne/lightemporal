import time
import uuid

from .core.context import ENV
from .workflow import activity


class Execution:
    def suspend_until(self, timestamp):
        time.sleep(max(timestamp - time.time(), 0))

    def suspend(self):
        pass

    def wake_up(self, id):
        pass


ENV['EXEC'] = Execution()


@activity
def _timestamp_for_duration(duration: int) -> float:
    return time.time() + duration


@activity
def _sleep_until(timestamp: float) -> None:
    if timestamp > time.time():
        ENV['EXEC'].suspend_until(timestamp)

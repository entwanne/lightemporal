import time

from .context import ENV
from .workflow import activity


class Execution:
    def suspend(self, timestamp):
        time.sleep(max(timestamp - time.time(), 0))


ENV['EXEC'] = Execution()


@activity
def _timestamp_for_duration(duration: int) -> float:
    return time.time() + duration


@activity
def _sleep_until(timestamp: float) -> None:
    if timestamp > time.time():
        ENV['EXEC'].suspend(timestamp)


def sleep(duration):
    return _sleep_until(_timestamp_for_duration(duration))

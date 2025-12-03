import time

from .workflow import activity

# contexts to use for specific actions
# if activity is being executed by a worker, sleep or wait signal should suspend the task on the worker until event has come
# in direct executor mode (default), actions are just normal ones

# but sleep(duraction) should still be converted to a sleep_until_timestamp activity (so that sleep is not restarted if the runner restarts)
# primitive could be a wait_for function that takes an event (TemporalEvent in case of sleep)

class DirectExecutorContext:
    def call(self, workflow, *args, **kwargs):
        return workflow(*args, **kwargs)

    def suspend(self, timestamp):
        time.sleep(max(timestamp - time.time(), 0))


class TaskExecutorContext:
    def call(self, workflow, *args, **kwargs):
        from tasks.queue import ENV
        ENV['Q'].put(workflow, *args, **kwargs)

    def suspend(self, timestamp):
        from tasks.exceptions import Suspend
        raise Suspend(timestamp=timestamp)


CTX = DirectExecutorContext()


@activity
def _timestamp_for_duration(duration: int) -> float:
    return time.time() + duration


@activity
def _sleep_until(timestamp: float) -> None:
    if timestamp > time.time():
        CTX.suspend(timestamp)


def sleep(duration):
    return _sleep_until(_timestamp_for_duration(duration))

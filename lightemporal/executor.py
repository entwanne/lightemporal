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


def sleep(duration):
    return _sleep_until(_timestamp_for_duration(duration))


def wait(name: str):
    from .models import Activity
    from .workflow import repos, workflow
    workflow_ctx = workflow.currents.get()[-1]
    workflow_id = workflow_ctx['id']
    workflow_ctx['step'] += 1
    step = workflow_ctx['step']
    if repos.signals.may_find_one(workflow_id, name, step):
        return
    ENV['EXEC'].suspend()


def signal(workflow_id: str, name: str):
    from .models import Signal
    from .workflow import repos
    repos.signals.new(Signal(workflow_id=workflow_id, name=name))
    ENV['EXEC'].wake_up(workflow_id)

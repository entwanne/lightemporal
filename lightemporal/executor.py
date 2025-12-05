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


def wait():
    from .models import Activity
    from .workflow import repos, workflow
    workflow_ctx = workflow.currents.get()[-1]
    workflow_id = workflow_ctx['id']
    workflow_ctx['step'] += 1
    step = workflow_ctx['step']
    activity = repos.activities.may_find_one(workflow_id, f'wait#{step}', '[[], {}]')
    if activity and activity.output == 'true':
        return
    activity = Activity(workflow_id=workflow_id, name=f'wait#{step}', input='[[], {}]', output='false')
    repos.activities.save(activity)
    ENV['EXEC'].suspend()


def signal(workflow_id):
    #from .models import Activity
    from .workflow import repos
    #activity = Activity(workflow_id=workflow_id, name='wait', input='[[], {}]', output='null')
    # get last run,ning activity and set to true
    #repos.activities.save(activity)
    for row in repos.activities.db.list(workflow_id=workflow_id):
        if row['name'].startswith('wait#') and row['output'] == 'false':
            row['output'] = 'true'
            repos.activities.db.set(row)
            ENV['EXEC'].wake_up(workflow_id)

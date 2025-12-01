import time

# contexts to use for specific actions
# if activity is being executed by a worker, sleep or wait signal should suspend the task on the worker until event has come
# in direct executor mode (default), actions are just normal ones

# but sleep(duraction) should still be converted to a sleep_until_timestamp activity (so that sleep is not restarted if the runner restarts)
# primitive could be a wait_for function that takes an event (TemporalEvent in case of sleep)

class DirectExecutorContext:
    def sleep(self, duration):
        time.sleep(duration)


class WorkerExecutorContext:
    def __init__(self):
        pass

    def sleep(self, duration):
        raise KeyboardInterrupt

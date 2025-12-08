import threading
import time
from collections import defaultdict
from contextlib import contextmanager

from .core.context import ENV


class DirectExecution:
    def suspend_until(self, workflow_id, timestamp):
        time.sleep(max(timestamp - time.time(), 0))

    def suspend(self, workflow_id):
        raise RuntimeError('Cannot suspend sync workflow')

    def on_signal(self, workflow_id, signal_cls, step, handler):
        raise RuntimeError('Sync workflow cannot receive signals')


class DirectRunner:
    def start(self, workflow, *args, **kwargs):
        raise RuntimeError('Cannot start async workflow with direct runner')

    def run(self, workflow, *args, **kwargs):
        with ENV.new_layer():
            ENV['EXEC'] = DirectExecution()
            w_id = workflow._create(*args, **kwargs)
            return workflow._run(w_id)

    def wake_up(self, id):
        raise RuntimeError('Cannot wake-up async workflow with direct runner')


class ThreadExecution(DirectExecution):
    events = defaultdict(threading.Event)

    def suspend(self, workflow_id):
        evt = self.events[workflow_id]
        evt.clear()
        evt.wait()

    def on_signal(self, workflow_id, signal_cls, step, handler):
        pass


class ThreadRunner:
    def start(self, workflow, *args, **kwargs):
        handler = Handler(workflow._run, workflow._create(*args, **kwargs))
        handler.start()
        return handler

    def run(self, workflow, *args, **kwargs):
        handler = self.start(workflow, *args, **kwargs)
        return handler.result()

    def wake_up(self, id):
        ThreadExecution.events[id].set()


class Handler:
    unset = object()

    def __init__(self, target, workflow_id):
        self._target = target
        self.workflow_id = workflow_id
        self.thread = threading.Thread(target=self.target, args=(dict(ENV),))
        self.ret = self.unset
        self.error = self.unset

    def target(self, parent_env):
        try:
            with ENV.new_layer():
                ENV.update(parent_env)
                ENV['EXEC'] = ThreadExecution()
                self.ret = self._target(self.workflow_id)
        except Exception as e:
            self.error = e

    def start(self):
        self.thread.start()

    def result(self):
        self.thread.join()
        if self.error is not self.unset:
            raise self.error
        assert self.ret is not self.unset
        return self.ret


ENV['RUN'] = DirectRunner()


@contextmanager
def thread_runner_env():
    with ENV.new_layer():
        ENV['RUN'] = ThreadRunner()
        yield

import threading
import time
from contextlib import contextmanager

from .core.context import ENV


class Execution:
    def suspend_until(self, timestamp):
        time.sleep(max(timestamp - time.time(), 0))

    def suspend(self):
        pass


class DirectRunner:
    def start(self, workflow, *args, **kwargs):
        raise RuntimeError('Cannot start async workflow with direct runner')

    def run(self, workflow, *args, **kwargs):
        w_id = workflow._create(*args, **kwargs)
        return workflow._run(w_id)

    def wake_up(self, id):
        pass


class ThreadRunner:
    def start(self, workflow, *args, **kwargs):
        handler = Handler(workflow._run, workflow._create(*args, **kwargs))
        handler.start()
        return handler

    def run(self, workflow, *args, **kwargs):
        handler = self.start(workflow, *args, **kwargs)
        return handler.result()


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


ENV['EXEC'] = Execution()
ENV['RUN'] = DirectRunner()


@contextmanager
def thread_runner_env():
    with ENV.new_layer():
        ENV['RUN'] = ThreadRunner()
        yield

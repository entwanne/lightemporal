import time


class Suspend(Exception):
    def __init__(self, *, timestamp=None, duration=None):
        super().__init__()
        self.timestamp = (time.time() if timestamp is None else timestamp) + (duration or 0)

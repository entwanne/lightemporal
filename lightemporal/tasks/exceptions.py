import time


class Suspend(Exception):
    def __init__(self, *, timestamp=None, duration=None):
        super().__init__()
        if timestamp is not None or duration is not None:
            self.timestamp = (time.time() if timestamp is None else timestamp) + (duration or 0)
        else:
            self.timestamp = None

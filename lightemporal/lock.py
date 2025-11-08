import time
from pathlib import Path


class FileLock:
    def __init__(self, path, block=True):
        self.path = Path(path)
        self.block = block
        self.file = None

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def acquire(self, block=None):
        if block is None:
            block = self.block

        while self.file is None:
            try:
                self.file = self.path.open('x')
            except FileExistsError:
                if block:
                    time.sleep(0.1)
                else:
                    raise

    def release(self):
        if self.file is not None:
            self.path.unlink()
            self.file.close()
            self.file = None

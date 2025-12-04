from lightemporal.worker import worker_env
from lightemporal.tasks.worker import run

from . import workflows


if __name__ == '__main__':
    with worker_env():
        run(workflows)

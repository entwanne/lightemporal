import sys

from lightemporal.worker import runner_env
from lightemporal.workflow import workflow

from .workflows import TestSignal


if __name__ == '__main__':
    with runner_env():
        workflow.signal(sys.argv[1], TestSignal(message=sys.argv[2] if len(sys.argv) > 2 else 'Hello'))

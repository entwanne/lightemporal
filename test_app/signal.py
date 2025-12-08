import sys

from lightemporal.worker import runner_env
from lightemporal.workflow import workflow


if __name__ == '__main__':
    with runner_env():
        workflow.signal(sys.argv[1], 'test_signal')

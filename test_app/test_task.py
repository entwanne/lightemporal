import functools
import sys

import random

from lightemporal.tasks.exceptions import Suspend


class TaskClass:
    def __init__(self, name):
        self.name = name

    @staticmethod
    def check() -> int:
        return 42

    def __call__(self, x: int) -> str:
        return f'={self.name}={x}='


tobj = TaskClass('tobj')

def pouet(x: int) -> str:
    ...

functools.update_wrapper(tobj, pouet)
pouet = tobj


def toto(value: str, count: int) -> str:
    if random.random() < 1/2:
        raise Suspend(duration=3)
    return ' : '.join([value] * count)


if __name__ == '__main__':
    print('Run worker with: python -m tasks.worker test_app.test_task')

    from lightemporal import ENV

    word = sys.argv[1] if len(sys.argv) > 1 else 'hello'
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 3

    Q = ENV['Q']
    print(Q.execute(toto, word, count))
    print(Q.execute(TaskClass.check))
    print(Q.execute(pouet, 5))

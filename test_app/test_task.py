import sys

import random

from tasks.exceptions import Suspend


def toto(value: str, count: int) -> str:
    if random.random() < 1/2:
        raise Suspend(duration=3)
    return ' : '.join([value] * count)


if __name__ == '__main__':
    print('Run worker with: python -m tasks.worker test_app.test_task:toto')

    from lightemporal.context import ENV

    word = sys.argv[1] if len(sys.argv) > 1 else 'hello'
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 3

    Q = ENV['Q']
    print(Q.execute(toto, word, count))

import random

from .exceptions import Suspend


def toto(value: str, count: int) -> str:
    if random.random() < 1/2:
        raise Suspend(duration=3)
    return ' : '.join([value] * count)


functions = {'toto': toto}

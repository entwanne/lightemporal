import sys
import types


def get_full_name(func):
    if isinstance(func, types.ModuleType):
        return func.__spec__.name
    module = func.__module__
    if module == '__main__':
        module = sys.modules[module].__spec__.name
    return f'{module}:{func.__qualname__}'

import importlib.metadata
import sys
import types


def get_task_name(target):
    if task_name := getattr(target, '__taskname__', None):
        return task_name

    if isinstance(target, types.ModuleType):
        return target.__spec__.name

    module = target.__module__
    if module == '__main__':
        module = sys.modules[module].__spec__.name

    return f'{module}:{target.__qualname__}'


def _recursive_discovery(tasks, basename, target):
    try:
        task_name = get_task_name(target)
    except AttributeError:
        return

    if not task_name.startswith(basename):
        return
    if task_name in tasks:
        return

    if hasattr(target, '__call__'):
        tasks[task_name] = target

    for name in {*getattr(target, '__dict__', ()), *getattr(type(target), '__dict__', ())}:
        if name.startswith('_'):
            continue
        attr = getattr(target, name, None)
        if attr is not None:
            _recursive_discovery(tasks, task_name, attr)


def _discover_from_entrypoints(entry_points):
    tasks = {}

    for ep in entry_points:
        target = ep.load()
        _recursive_discovery(tasks, ep.value, target)

    return tasks


def discover():
    return _discover_from_entrypoints(importlib.metadata.entry_points().select(group='tasks'))


def discover_from_names(*names):
    return _discover_from_entrypoints(
        importlib.metadata.EntryPoint(name, value=name, group='tasks')
        for name in names
    )


def load():
    tasks = {}

    for ep in importlib.metadata.entry_points().select(group='task_discovers'):
        tasks |= ep.load()()

    return tasks

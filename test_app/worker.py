import time

from lightemporal import executor
from tasks.exceptions import Suspend
from tasks.queue import Q

from .workflows import payment_workflow, issue_refund, apply_refund


executor.CTX = executor.TaskExecutorContext()


def run(**functions):
    while True:
        func, args, kwargs = Q.get(functions)
        print(func, args, kwargs)
        try:
            print(func(*args, **kwargs))
        except Suspend as e:
            print(f'{func.__name__} suspended for {round(max(e.timestamp - time.time(), 0))}s')
            Q.call_at(func, e.timestamp, *args, **kwargs)
        except Exception as e:
            print(f'{func.__name__} failed: {e!r}')
            Q.put(func, *args, **kwargs)


if __name__ == '__main__':
    run(payment_workflow=payment_workflow, issue_refund=issue_refund, apply_refund=apply_refund)

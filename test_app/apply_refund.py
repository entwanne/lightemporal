import sys

from lightemporal.worker import task_runner

from .workflows import apply_refund

#apply_refund(sys.argv[1])
with task_runner():
    print(apply_refund(sys.argv[1]))

import sys

from lightemporal.executor import TaskExecutorContext
ctx = TaskExecutorContext()

from .workflows import apply_refund

#apply_refund(sys.argv[1])
ctx.call(apply_refund, sys.argv[1])

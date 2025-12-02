import sys

from lightemporal.executor import TaskExecutorContext
ctx = TaskExecutorContext()

from .workflows import issue_refund

#print(issue_refund(sys.argv[1], int(sys.argv[2])))
ctx.call(issue_refund, sys.argv[1], int(sys.argv[2]))

import sys

from lightemporal.worker import task_runner

from .workflows import issue_refund

#print(issue_refund(sys.argv[1], int(sys.argv[2])))
with task_runner():
    issue_refund(sys.argv[1], int(sys.argv[2]))

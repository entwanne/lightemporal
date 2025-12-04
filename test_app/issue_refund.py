import sys

from lightemporal.worker import runner_env

from .workflows import issue_refund

#print(issue_refund(sys.argv[1], int(sys.argv[2])))
with runner_env():
    #print(issue_refund(sys.argv[1], int(sys.argv[2])))
    handler = issue_refund.start(sys.argv[1], int(sys.argv[2]))
    print(handler, vars(handler))
    print(handler.result())

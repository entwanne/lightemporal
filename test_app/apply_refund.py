import sys

from lightemporal.worker import runner_env

from .workflows import apply_refund

#apply_refund(sys.argv[1])
with runner_env():
    #print(apply_refund(sys.argv[1]))
    handler = apply_refund.start(sys.argv[1])
    print(handler, vars(handler))
    print(handler.result())

import sys

from .workflows import issue_refund

print(issue_refund(sys.argv[1], int(sys.argv[2])))

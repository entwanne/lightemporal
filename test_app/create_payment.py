import sys

from .workflows import payments

print(repr(payments.create(amount=sys.argv[1])))

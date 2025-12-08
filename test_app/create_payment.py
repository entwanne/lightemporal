import sys

from .workflows import payments

print(payments.create(amount=sys.argv[1]).id)

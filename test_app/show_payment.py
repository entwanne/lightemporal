import sys

from .workflows import payments, refunds

def show_payment(payment):
    print(repr(payment))
    print('Refundable amount:', payments.get_refundable_amount(payment.id))
    print('Rebatable amount:', payments.get_rebatable_amount(payment.id))
    print('Returnable amount:', payments.get_returnable_amount(payment.id))
    print('Refunds:')
    for refund in refunds.list_for_payment(payment):
        print('   ', repr(refund))

if len(sys.argv) > 1:
    show_payment(payments.get(sys.argv[1]))
else:
    for p in payments.list():
        show_payment(p)
        print('=' * 20)

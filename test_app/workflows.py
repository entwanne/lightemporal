import random

from lightemporal import activity, workflow
from lightemporal.backend import DB
from lightemporal.executor import sleep

from .models import Payment, Refund
from .repos import PaymentRepository, RefundRepository

payments = PaymentRepository(DB)
refunds = RefundRepository(DB)


def may_fail():
    if random.random() < 1/3:
        raise ValueError


@workflow
def payment_workflow(payment_id: str) -> None:
    raise RuntimeError('Do not call as a workflow')


@workflow
def issue_refund(payment_id: str, amount: int) -> str:
    print('Sleeping for 5s')
    sleep(5)
    print('End sleep')

    if not check_payment_id(payment_id):
        return ''
    with payment_workflow.use(payment_id):
        return init_refund(payment_id, amount)


@workflow
def apply_refund(refund_id: str) -> int:
    print('Sleeping for 5s')
    sleep(5)
    print('End sleep')

    payment_id = get_payment_id(refund_id)
    with payment_workflow.use(payment_id):
        rebate_amount = apply_rebate(refund_id)
        return_amount = apply_return(refund_id)
        check_refund(refund_id, rebate_amount, return_amount)
        return rebate_amount + return_amount


@activity
def check_payment_id(payment_id: str) -> bool:
    try:
        may_fail()
        payments.get(payment_id)
    except Exception:
        return False
    else:
        return True


@activity
def get_payment_id(refund_id: str) -> str:
    refund = refunds.get(refund_id)
    print(repr(refund))
    may_fail()
    return refund.payment_id


@activity
def init_refund(payment_id: str, amount: int) -> str:
    payment = payments.get(payment_id)
    if amount > payments.get_refundable_amount(payment.id):
        raise ValueError(f'Cannot refund {amount} on payment')
    may_fail()
    refund = refunds.create(payment=payment, requested_amount=amount)
    print(repr(refund))
    return refund.id


@activity
def apply_rebate(refund_id: str) -> int:
    print('Rebate')
    refund = refunds.get(refund_id)
    print(repr(refund))
    if refund.rebate_amount > 0 or refund.return_amount > 0:
        return refund.rebate_amount
    rebate_amount = min(refund.requested_amount, payments.get_rebatable_amount(refund.payment_id))
    print('Rebating', rebate_amount)
    refund.rebate_amount = rebate_amount
    print(repr(refund))
    may_fail()
    refunds.update(refund)
    may_fail()
    return rebate_amount


@activity
def apply_return(refund_id: str) -> int:
    print('Return')
    refund = refunds.get(refund_id)
    print(repr(refund))
    if refund.return_amount > 0 or refund.requested_amount == refund.rebate_amount + refund.return_amount:
        return refund.return_amount
    return_amount = min(refund.requested_amount - refund.rebate_amount, payments.get_returnable_amount(refund.payment_id))
    print('Returning', return_amount)
    refund.return_amount = return_amount
    print(repr(refund))
    may_fail()
    refunds.update(refund)
    may_fail()
    return return_amount


@activity
def check_refund(refund_id: str, rebate_amount: int, return_amount: int) -> bool:
    refund = refunds.get(refund_id)
    print(repr(refund))
    assert refund.rebate_amount == rebate_amount
    assert refund.return_amount == return_amount
    assert refund.rebate_amount + refund.return_amount == refund.requested_amount
    return True

from typing import Annotated

import pydantic
from lightemporal import activity, workflow
from lightemporal.models import UUID


class Payment(pydantic.BaseModel):
    id: UUID


class Refund(pydantic.BaseModel):
    payment_id: str
    id: UUID
    requested_amount: int = 0
    rebate_amount: int = 0
    refund_amount: int = 0


@workflow
def payment_workflow(payment_id: str):
    raise RuntimeError('Do not call as a workflow')


@workflow
def refund_payment(payment_id: str, amount: int):
    with payment_workflow.use(payment_id):
        refund = init_refund(payment_id, amount)
        print(refund)
        raise ValueError


@activity
def init_refund(payment_id: str, amount: int):
    return Refund(payment_id=payment_id, requested=amount)


@activity
def rebate_amount(refund_id: str, amount: int) -> int:
    #amount = 
    pass


if __name__ == '__main__':
    payment = Payment(id='cbb8d70a-4564-4cd6-ac35-04ebdae1e575')
    refund_payment(payment.id, 100_00)

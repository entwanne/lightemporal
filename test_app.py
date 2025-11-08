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
def refund_payment(payment_id: str, amount: int):
    pass


@activity
def rebate_amount(refund: str, amount: int) -> int:
    #amount = 
    pass


if __name__ == '__main__':
    payment = Payment(id='cbb8d70a-4564-4cd6-ac35-04ebdae1e575')
    refund_payment(payment.id, 100_00)

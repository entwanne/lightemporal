import pydantic

from lightemporal.models import UUID


class Payment(pydantic.BaseModel):
    id: UUID
    amount: int = 0


class Refund(pydantic.BaseModel):
    payment_id: str
    id: UUID
    requested_amount: int = 0
    rebate_amount: int = 0
    return_amount: int = 0

from collections.abc import Iterable
from functools import cached_property

from lightemporal import ENV

from .models import Payment, Refund


class PaymentRepository:
    def __init__(self, db):
        self.payment_db = db.tables['payments']
        self.refund_db = db.tables['refunds']

    def list(self) -> Iterable[Payment]:
        for row in self.payment_db.list():
            yield Payment.model_validate(row)

    def get(self, id: str) -> Payment:
        return Payment.model_validate(self.payment_db.get(id))

    def create(self, **kwargs) -> Payment:
        payment = Payment(**kwargs)
        self.payment_db.set(payment.model_dump(mode='json'))
        return payment

    def get_refundable_amount(self, id: str) -> int:
        payment = self.get(id)
        refunded_amount = sum(r['requested_amount'] for r in self.refund_db.list(payment_id=id))
        return max(payment.amount - refunded_amount, 0)

    def get_rebatable_amount(self, id: str) -> int:
        payment = self.get(id)
        rebatable_amount = payment.amount // 3
        rebated_amount = sum(r['rebate_amount'] for r in self.refund_db.list(payment_id=id))
        return max(rebatable_amount - rebated_amount, 0)

    def get_returnable_amount(self, id: str) -> int:
        payment = self.get(id)
        returnable_amount = payment.amount - payment.amount // 3
        returned_amount = sum(r['return_amount'] for r in self.refund_db.list(payment_id=id))
        return max(returnable_amount - returned_amount, 0)


class RefundRepository:
    def __init__(self, db):
        self.db = db.tables['refunds']

    def list_for_payment(self, payment: Payment) -> Iterable[Refund]:
        for row in self.db.list(payment_id=payment.id):
            yield Refund.model_validate(row)

    def get(self, id: str) -> Refund:
        return Refund.model_validate(self.db.get(id))

    def create(self, payment: Payment, requested_amount: int = 0) -> Refund:
        refund = Refund(payment_id=payment.id, requested_amount=requested_amount)
        self.db.set(refund.model_dump(mode='json'))
        return refund

    def update(self, refund: Refund):
        self.db.set(refund.model_dump(mode='json'))


class Repositories:
    @cached_property
    def payments(self):
        return PaymentRepository(ENV['DB'])

    @cached_property
    def refunds(self):
        return RefundRepository(ENV['DB'])

from .models import Payment, Refund


class PaymentRepository:
    def __init__(self, db):
        self.db = db

    def get(self, id: str) -> Payment:
        return Payment.model_validate(self.db.get('payments', id))

    def create(self, **kwargs) -> Payment:
        payment = Payment(**kwargs)
        self.db.set('payments', payment.model_dump(mode='json'))
        return payment

    def get_refundable_amount(self, id: str) -> int:
        payment = self.get(id)
        refunded_amount = sum(r['requested_amount'] for r in self.db.list('refunds', payment_id=id))
        return max(payment.amount - refunded_amount, 0)

    def get_rebatable_amount(self, id: str) -> int:
        payment = self.get(id)
        rebatable_amount = payment.amount // 3
        rebated_amount = sum(r['rebate_amount'] for r in self.db.list('refunds', payment_id=id))
        return max(rebatable_amount - rebated_amount, 0)

    def get_returnable_amount(self, id: str) -> int:
        payment = self.get(id)
        returnable_amount = payment.amount - payment.amount // 3
        returned_amount = sum(r['return_amount'] for r in self.db.list('refunds', payment_id=id))
        return max(returnable_amount - returned_amount, 0)


class RefundRepository:
    def __init__(self, db):
        self.db = db

    def get(self, id: str) -> Refund:
        return Refund.model_validate(self.db.get('refunds', id))

    def create(self, payment: Payment, requested_amount: int = 0) -> Refund:
        refund = Refund(payment_id=payment.id, requested_amount=requested_amount)
        self.db.set('refunds', refund.model_dump(mode='json'))
        return refund

    def update(self, refund: Refund):
        self.db.set('refunds', refund.model_dump(mode='json'))

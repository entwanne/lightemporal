from collections.abc import Iterable
from functools import cached_property

from lightemporal import ENV

from .models import Payment, Refund


class PaymentRepository:
    def __init__(self, db):
        self.db = db
        db.declare_table('payments', Payment)
        RefundRepository(db)

    def list(self) -> Iterable[Payment]:
        yield from self.db.query('SELECT * FROM payments', model=Payment)

    def get(self, id: str) -> Payment:
        return self.db.query_one('SELECT * FROM payments WHERE id = ?', (id,), model=Payment)

    def create(self, **kwargs) -> Payment:
        return self.db.query_one(
            'INSERT INTO payments VALUES (:id, :amount) RETURNING *',
            Payment(**kwargs),
            commit=True,
            model=Payment,
        )

    def get_refundable_amount(self, id: str) -> int:
        return self.db.query_one(
            """
            SELECT amount - coalesce((SELECT SUM(requested_amount) FROM refunds WHERE payment_id = :id), 0) AS amount
            FROM payments
            WHERE id = :id
            """,
            (id,),
        )['amount']

    def get_rebatable_amount(self, id: str) -> int:
        return self.db.query_one(
            """
            SELECT amount/3 - coalesce((SELECT SUM(rebate_amount) FROM refunds WHERE payment_id = :id), 0) AS amount
            FROM payments
            WHERE id = :id
            """,
            (id,),
        )['amount']

    def get_returnable_amount(self, id: str) -> int:
        return self.db.query_one(
            """
            SELECT amount - amount/3 - coalesce((SELECT SUM(return_amount) FROM refunds WHERE payment_id = :id), 0) AS amount
            FROM payments
            WHERE id = :id
            """,
            (id,),
        )['amount']


class RefundRepository:
    def __init__(self, db):
        self.db = db
        db.declare_table('refunds', Refund)

    def list_for_payment(self, payment: Payment) -> Iterable[Refund]:
        yield from self.db.query(
            'SELECT * FROM refunds WHERE payment_id = :id',
            payment,
            model=Refund,
        )

    def get(self, id: str) -> Refund:
        return self.db.query_one(
            'SELECT * FROM refunds WHERE id = ?',
            (id,),
            model=Refund,
        )

    def create(self, payment: Payment, requested_amount: int = 0) -> Refund:
        return self.db.query_one(
            'INSERT INTO refunds VALUES (:payment_id, :id, :requested_amount, :rebate_amount, :return_amount) RETURNING *',
            Refund(payment_id=payment.id, requested_amount=requested_amount),
            commit=True,
            model=Refund,
        )

    def update(self, refund: Refund):
        self.db.execute(
            'UPDATE refunds SET requested_amount = :requested_amount, rebate_amount = :rebate_amount, return_amount = :return_amount WHERE id = :id',
            refund,
            commit=True,
        )


class Repositories:
    @cached_property
    def payments(self):
        return PaymentRepository(ENV['DB'])

    @cached_property
    def refunds(self):
        return RefundRepository(ENV['DB'])

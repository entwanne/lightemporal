from lightemporal.worker import worker

from .workflows import payment_workflow, issue_refund, apply_refund


if __name__ == '__main__':
    worker(
        payment_workflow=payment_workflow,
        issue_refund=issue_refund,
        apply_refund=apply_refund,
    )

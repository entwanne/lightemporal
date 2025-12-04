from lightemporal.worker import worker_env, discover_tasks_from_workflows
from lightemporal.tasks.worker import run

from .workflows import payment_workflow, issue_refund, apply_refund


if __name__ == '__main__':
    with worker_env():
        run(**discover_tasks_from_workflows(payment_workflow, issue_refund, apply_refund))

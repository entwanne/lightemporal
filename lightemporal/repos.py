from .models import Workflow, WorkflowStatus


class WorkflowRepository:
    def __init__(self, db):
        self.db = db

    def get_or_create(self, name: str, input: str) -> Workflow:
        with self.db.atomic:
            for row in self.db.list('workflows', name=name, input=input):
                workflow = Workflow.model_validate(row)
                match workflow.status:
                    case WorkflowStatus.STOPPED:
                        return workflow
                    case WorkflowStatus.COMPLETED:
                        continue
                    case WorkflowStatus.RUNNING:
                        raise ValueError('Workflow is already running')
            workflow = Workflow(name=name, input=input)
            self.db.set('workflows', workflow.model_dump(mode='json'))
            return workflow

    def complete(self, workflow: Workflow) -> Workflow:
        workflow.status = WorkflowStatus.COMPLETED
        self.db.set('workflows', workflow.model_dump(mode='json'))
        return workflow

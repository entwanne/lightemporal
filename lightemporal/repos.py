from .models import Workflow, WorkflowStatus, Activity


class WorkflowRepository:
    def __init__(self, db):
        self.db = db.tables['workflows']

    def get_or_create(self, name: str, input: str, ok_stopped: bool = True) -> Workflow:
        with self.db.atomic:
            for row in self.db.list(name=name, input=input, status='RUNNING'):
                raise ValueError('Workflow is already running')

            for row in self.db.list(name=name, input=input, status='STOPPED'):
                workflow = Workflow.model_validate(row)
                if not ok_stopped:
                    raise ValueError('Another stopped workflow already exists')
                workflow.status = WorkflowStatus.RUNNING
                self.db.set(workflow.model_dump(mode='json'))
                return workflow
                workflow = Workflow.model_validate(row)

            workflow = Workflow(name=name, input=input)
            self.db.set(workflow.model_dump(mode='json'))
            return workflow

    def complete(self, workflow: Workflow) -> Workflow:
        workflow.status = WorkflowStatus.COMPLETED
        self.db.set(workflow.model_dump(mode='json'))
        return workflow

    def failed(self, workflow: Workflow) -> Workflow:
        workflow.status = WorkflowStatus.STOPPED
        self.db.set(workflow.model_dump(mode='json'))
        return workflow


class ActivityRepository:
    def __init__(self, db):
        self.db = db.tables['activities']

    def save(self, activity: Activity) -> None:
        self.db.set(activity.model_dump(mode='json'))

    def may_find_one(self, workflow_id, name, input) -> Activity | None:
        for row in self.db.list(workflow_id=workflow_id, name=name, input=input):
            return Activity.model_validate(row)
        return None

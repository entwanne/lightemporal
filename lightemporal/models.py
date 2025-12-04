import enum
from typing import Annotated
from uuid import uuid4

import pydantic


UUID = Annotated[str, pydantic.Field(default_factory=lambda: str(uuid4()))]


class WorkflowStatus(enum.Enum):
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    STOPPED = 'STOPPED'


class Workflow(pydantic.BaseModel):
    id: UUID
    name: str
    input: str
    status: WorkflowStatus = WorkflowStatus.RUNNING


class Activity(pydantic.BaseModel):
    id: UUID
    workflow_id: UUID
    name: str
    input: str
    output: str

import enum

import pydantic

from .core.utils import UUID


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


class Signal(pydantic.BaseModel):
    id: UUID
    workflow_id: UUID
    name: str
    content: dict
    step: int | None = None

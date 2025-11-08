import enum
import inspect
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
    status: WorkflowStatus = WorkflowStatus.STOPPED


class ActivityResult(pydantic.BaseModel):
    workflow_id: UUID
    name: str
    input: str
    output: str


def param_types(f):
    sig = inspect.signature(f)

    args = tuple[*(
        p.annotation
        for p in sig.parameters.values()
        if p.kind is not p.KEYWORD_ONLY
    )]
    kwargs = pydantic.create_model(
        'kwargs',
        **{
            p.name: p.annotation
            for p in sig.parameters.values()
            if p.kind is p.KEYWORD_ONLY
        }
    )
    return args, kwargs

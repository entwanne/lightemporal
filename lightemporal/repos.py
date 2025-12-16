from functools import cached_property

from .core.context import ENV
from .models import Workflow, WorkflowStatus, Activity, Signal


class WorkflowRepository:
    def __init__(self, db):
        self.db = db
        db.declare_table('workflows', Workflow)

    def get_or_create(self, name: str, input: str) -> Workflow:
        with self.db.cursor(commit=True):
            for _ in self.db.query(
                    """
                    SELECT 1 FROM workflows
                    WHERE name = ? AND input = ? AND status = 'RUNNING'
                    """,
                    (name, input),
            ):
                raise ValueError('Workflow is already running')

            for row in self.db.query(
                    """
                    UPDATE workflows
                    SET status = 'RUNNING'
                    WHERE id = (
                        SELECT id FROM workflows
                        WHERE name = ? AND input = ? AND status = 'STOPPED'
                        LIMIT 1
                    )
                    RETURNING *
                    """,
                    (name, input),
                    model=Workflow,
            ):
                return row

            return self.db.query_one(
                "INSERT INTO workflows VALUES (:id, :name, :input, :status) RETURNING *",
                Workflow(name=name, input=input),
                model=Workflow,
            )

    def get(self, workflow_id: str) -> Workflow:
        return self.db.query_one("SELECT * FROM workflows WHERE id = ?", (workflow_id,), model=Workflow)

    def complete(self, workflow: Workflow) -> Workflow:
        return self.db.query_one(
            "UPDATE workflows SET status = 'COMPLETED' WHERE id = :id RETURNING *",
            workflow,
            commit=True,
            model=Workflow,
        )

    def failed(self, workflow: Workflow) -> Workflow:
        return self.db.query_one(
            "UPDATE workflows SET status = 'STOPPED' WHERE id = :id RETURNING *",
            workflow,
            commit=True,
            model=Workflow,
        )


class ActivityRepository:
    def __init__(self, db):
        self.db = db
        db.declare_table('activities', Activity)

    def save(self, activity: Activity) -> None:
        self.db.execute(
            """
            INSERT INTO activities
            VALUES (:id, :workflow_id, :name, :input, :output)
            ON CONFLICT (id) DO UPDATE
            SET output = :output
            """,
            activity,
            commit=True,
        )

    def may_find_one(self, workflow_id, name, input) -> Activity | None:
        for row in self.db.query(
                "SELECT * FROM activities WHERE workflow_id = ? AND name = ? AND input = ?",
                (workflow_id, name, input),
                model=Activity,
        ):
            return row
        return None


class SignalRepository:
    def __init__(self, db):
        self.db = db
        db.declare_table('signals', Signal)

    def new(self, signal: Signal) -> None:
        self.db.execute(
            "INSERT INTO signals VALUES (:id, :workflow_id, :name, :content, :step)",
            signal,
            commit=True,
        )

    def may_find_one(self, workflow_id: str, name: str, step: int) -> Signal | None:
        with self.db.cursor(commit=True):
            data = {'workflow_id': workflow_id, 'name': name, 'step': step}

            for row in self.db.query(
                    "SELECT * FROM signals WHERE workflow_id = ? AND name = ? AND step = ?",
                    tuple(data.values()),
                    model=Signal,
            ):
                return row

            for row in self.db.query(
                    """
                    UPDATE signals
                    SET step = :step
                    WHERE id = (
                        SELECT id FROM signals
                        WHERE workflow_id = :workflow_id AND name = :name AND step IS NULL
                        LIMIT 1
                    )
                    RETURNING *
                    """,
                    data,
                    model=Signal,
            ):
                return row

        return None


class Repositories:
    @cached_property
    def workflows(self):
        return WorkflowRepository(ENV['DB'])

    @cached_property
    def activities(self):
        return ActivityRepository(ENV['DB'])

    @cached_property
    def signals(self):
        return SignalRepository(ENV['DB'])

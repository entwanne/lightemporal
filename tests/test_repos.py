import pytest

from lightemporal.core.backend import Backend
from lightemporal.core.context import ENV
from lightemporal.models import WorkflowStatus, Workflow, Activity, Signal
from lightemporal.repos import WorkflowRepository, ActivityRepository, SignalRepository, Repositories


@pytest.fixture()
def env():
    with ENV.new_layer():
        return ENV


@pytest.fixture()
def db(env):
    with Backend(':memory:') as database:
        env['DB'] = database
        yield database


@pytest.fixture()
def repos(db):
    return Repositories()


def test_workflow_repository(repos, db):
    assert isinstance(repos.workflows, WorkflowRepository)
    assert repos.workflows.db is db

    assert {row['sql'] for row in db.query("SELECT * FROM sqlite_schema WHERE tbl_name = 'workflows'")} == {
        'CREATE TABLE workflows (id, name, input, status)',
        'CREATE UNIQUE INDEX ux_workflows_id ON workflows(id)',
    }

    assert {r['id'] for r in db.query('SELECT id FROM workflows')} == set()

    # Get or create

    workflow = repos.workflows.get_or_create('test-workflow', '[]')
    assert workflow.id
    assert workflow.name == 'test-workflow'
    assert workflow.input == '[]'
    assert workflow.status is WorkflowStatus.RUNNING

    assert {r['id'] for r in db.query('SELECT id FROM workflows')} == {workflow.id}
    assert db.query_one('SELECT * FROM workflows WHERE id = ?', (workflow.id,)) == {
        'id': workflow.id,
        'name': 'test-workflow',
        'input': '[]',
        'status': 'RUNNING'
    }

    # Transaction is committed
    db.connection.rollback()
    assert {r['id'] for r in db.query('SELECT id FROM workflows')} == {workflow.id}

    with pytest.raises(ValueError) as err:
        repos.workflows.get_or_create('test-workflow', '[]')
    assert str(err.value) == 'Workflow is already running'

    workflow2 = repos.workflows.get_or_create('test-workflow2', '[]')
    assert workflow2.id != workflow.id
    workflow3 = repos.workflows.get_or_create('test-workflow', '[1,2]')
    assert workflow3.id != workflow.id
    assert {r['id'] for r in db.query('SELECT id FROM workflows')} == {workflow.id, workflow2.id, workflow3.id}

    # Get

    result = repos.workflows.get(workflow.id)
    assert result == workflow

    # Complete / failed

    result = repos.workflows.complete(workflow)
    assert result == workflow.model_copy(update={'status': WorkflowStatus.COMPLETED})
    assert repos.workflows.get(workflow.id) == result

    db.connection.rollback()
    assert repos.workflows.get(workflow.id) == result

    result = repos.workflows.failed(workflow)
    assert result == workflow.model_copy(update={'status': WorkflowStatus.STOPPED})
    assert repos.workflows.get(workflow.id) == result

    db.connection.rollback()
    assert repos.workflows.get(workflow.id) == result

    # Get or create - update existing workflow

    result = repos.workflows.get_or_create('test-workflow', '[]')
    assert result.id == workflow.id
    assert result.status is WorkflowStatus.RUNNING

    repos.workflows.complete(workflow)

    result = repos.workflows.get_or_create('test-workflow', '[]')
    assert result.id != workflow.id
    assert result.status is WorkflowStatus.RUNNING

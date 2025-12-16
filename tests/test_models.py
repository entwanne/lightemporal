import pytest

from lightemporal.models import WorkflowStatus, Workflow, Activity, Signal


def test_workflow():
    workflow = Workflow(
        name='test-workflow',
        input='[3, 5]'
    )
    assert isinstance(workflow.id, str) and len(workflow.id) > 0
    assert workflow.name == 'test-workflow'
    assert workflow.input == '[3, 5]'
    assert workflow.status is WorkflowStatus.RUNNING


@pytest.mark.parametrize(
    'input_status,expected_status',
    [
        ('RUNNING', WorkflowStatus.RUNNING),
        ('COMPLETED', WorkflowStatus.COMPLETED),
        ('STOPPED', WorkflowStatus.STOPPED),
    ]
)
def test_workflow_status(input_status, expected_status):
    workflow = Workflow(
        id='workflow-id',
        name='test-workflow',
        input='[3, 5]',
        status=input_status,
    )
    assert workflow.id == 'workflow-id'
    assert workflow.status is expected_status


def test_activity():
    activity = Activity(
        workflow_id='workflow-id',
        name='test-activity',
        input='3',
        output='[]',
    )
    assert isinstance(activity.id, str) and len(activity.id) > 0
    assert activity.workflow_id == 'workflow-id'
    assert activity.name == 'test-activity'
    assert activity.input == '3'
    assert activity.output == '[]'


def test_signal():
    signal = Signal(
        workflow_id='workflow-id',
        name='test-signal',
        content='[1,2,3]',
    )
    assert isinstance(signal.id, str) and len(signal.id) > 0
    assert signal.workflow_id == 'workflow-id'
    assert signal.name == 'test-signal'
    assert signal.content == '[1,2,3]'
    assert signal.step is None


def test_signal_step():
    signal = Signal(
        workflow_id='workflow-id',
        name='test-signal',
        content='null',
        step=4,
    )
    assert signal.step == 4

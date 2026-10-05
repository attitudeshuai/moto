"""
End-to-end tests for orchestrated state transitions: failure injection reuses
the existing failure status/reason fields of each service, and groups of
resources can be progressed in dependency order.
"""

from unittest import SkipTest

import boto3
import pytest
from botocore.exceptions import ClientError

from moto import mock_aws, settings
from moto.athena.models import athena_backends
from moto.core import DEFAULT_ACCOUNT_ID
from moto.dms.models import dms_backends
from moto.moto_api import OrchestrationError, OrchestrationPlan, state_manager
from moto.pipes.models import pipes_backends
from moto.transcribe.models import transcribe_backends

REGION = "eu-west-1"


@pytest.fixture(autouse=True)
def _decorator_mode_only():
    if not settings.TEST_DECORATOR_MODE:
        raise SkipTest("Orchestration is only available in-process (decorator mode)")


@pytest.fixture
def manual_transitions():
    state_manager.set_transition(
        "transcribe::transcriptionjob",
        transition={"progression": "manual", "times": 1},
    )
    state_manager.set_transition(
        "transcribe::vocabulary", transition={"progression": "manual", "times": 1}
    )
    state_manager.set_transition(
        "athena::execution", transition={"progression": "manual", "times": 1}
    )
    yield
    state_manager.unset_transition("transcribe::transcriptionjob")
    state_manager.unset_transition("transcribe::vocabulary")
    state_manager.unset_transition("athena::execution")


# ---------------------------------------------------------------------------
# Transcribe - FAILED + FailureReason
# ---------------------------------------------------------------------------
@mock_aws
def test_transcribe_job_failure_is_reported_with_failure_reason(manual_transitions):
    client = boto3.client("transcribe", region_name=REGION)
    client.start_transcription_job(
        TranscriptionJobName="job1",
        LanguageCode="en-US",
        MediaFormat="mp3",
        Media={"MediaFileUri": "s3://bucket/file.mp3"},
    )

    job = transcribe_backends[DEFAULT_ACCOUNT_ID][REGION].transcriptions["job1"]
    job.fail_at("IN_PROGRESS", reason="The media file could not be read")

    # First describe: QUEUED -> IN_PROGRESS is where the failure is injected
    assert (
        client.get_transcription_job(TranscriptionJobName="job1")["TranscriptionJob"][
            "TranscriptionJobStatus"
        ]
        == "QUEUED"
    )
    response = client.get_transcription_job(TranscriptionJobName="job1")[
        "TranscriptionJob"
    ]
    assert response["TranscriptionJobStatus"] == "FAILED"
    assert response["FailureReason"] == "The media file could not be read"

    # The job is frozen and does not progress automatically anymore
    response = client.get_transcription_job(TranscriptionJobName="job1")[
        "TranscriptionJob"
    ]
    assert response["TranscriptionJobStatus"] == "FAILED"
    assert job.is_frozen is True
    assert job.remaining_statuses == []


@mock_aws
def test_transcribe_vocabulary_failure(manual_transitions):
    client = boto3.client("transcribe", region_name=REGION)
    client.create_vocabulary(
        VocabularyName="vocab1",
        LanguageCode="en-US",
        Phrases=["hello", "world"],
    )

    vocabulary = transcribe_backends[DEFAULT_ACCOUNT_ID][REGION].vocabularies["vocab1"]
    vocabulary.fail_at("PENDING", reason="Invalid phrase")

    response = client.get_vocabulary(VocabularyName="vocab1")
    assert response["VocabularyState"] == "FAILED"
    assert response["FailureReason"] == "Invalid phrase"


# ---------------------------------------------------------------------------
# Athena - FAILED + StateChangeReason
# ---------------------------------------------------------------------------
@mock_aws
def test_athena_execution_failure(manual_transitions):
    client = boto3.client("athena", region_name=REGION)
    exec_id = client.start_query_execution(
        QueryString="SELECT stuff FROM mytable",
        QueryExecutionContext={"Database": "default", "Catalog": "awsdatacatalog"},
        ResultConfiguration={"OutputLocation": "s3://bucket/results"},
    )["QueryExecutionId"]

    execution = athena_backends[DEFAULT_ACCOUNT_ID][REGION].executions[exec_id]
    execution.fail_at("RUNNING", reason="SYNTAX_ERROR")

    status = client.get_query_execution(QueryExecutionId=exec_id)["QueryExecution"][
        "Status"
    ]
    assert status["State"] == "FAILED"
    assert status["StateChangeReason"] == "SYNTAX_ERROR"

    # Upper-level code branching on a failed query is triggered for real
    with pytest.raises(ClientError):
        client.get_query_results(QueryExecutionId=exec_id)


# ---------------------------------------------------------------------------
# DMS - failed + LastFailureMessage
# ---------------------------------------------------------------------------
@mock_aws
def test_dms_connection_failure_uses_last_failure_message():
    client = boto3.client("dms", region_name=REGION)
    instance_arn = client.create_replication_instance(
        ReplicationInstanceIdentifier="test-instance",
        ReplicationInstanceClass="dms.t2.micro",
    )["ReplicationInstance"]["ReplicationInstanceArn"]
    endpoint_arn = client.create_endpoint(
        EndpointIdentifier="test-endpoint",
        EndpointType="source",
        EngineName="mysql",
    )["Endpoint"]["EndpointArn"]
    client.test_connection(
        ReplicationInstanceArn=instance_arn, EndpointArn=endpoint_arn
    )

    connection = dms_backends[DEFAULT_ACCOUNT_ID][REGION].connections[0]
    connection.fail_at("testing", reason="Could not connect to the database")

    connection_dict = client.describe_connections()["Connections"][0]
    assert connection_dict["Status"] == "failed"
    assert connection_dict["LastFailureMessage"] == (
        "Could not connect to the database"
    )

    # Frozen - more describes do not move the connection to 'successful'
    connection_dict = client.describe_connections()["Connections"][0]
    assert connection_dict["Status"] == "failed"


# ---------------------------------------------------------------------------
# EventBridge Pipes - CREATE_FAILED + StateReason
# ---------------------------------------------------------------------------
def _create_pipe(client, name: str) -> None:
    client.create_pipe(
        Name=name,
        Source=f"arn:aws:sqs:{REGION}:{DEFAULT_ACCOUNT_ID}:queue",
        Target=f"arn:aws:lambda:{REGION}:{DEFAULT_ACCOUNT_ID}:function:fn",
        RoleArn=f"arn:aws:iam::{DEFAULT_ACCOUNT_ID}:role/role",
    )


@mock_aws
def test_pipe_create_failure_uses_state_reason():
    client = boto3.client("pipes", region_name=REGION)
    _create_pipe(client, "pipe1")

    pipe = pipes_backends[DEFAULT_ACCOUNT_ID][REGION].pipes["pipe1"]
    pipe.fail_at("CREATING", reason="The role could not be assumed")

    response = client.describe_pipe(Name="pipe1")
    assert response["CurrentState"] == "CREATE_FAILED"
    assert response["StateReason"] == "The role could not be assumed"


@mock_aws
def test_orchestration_plan_progresses_pipes_in_dependency_order():
    client = boto3.client("pipes", region_name=REGION)
    _create_pipe(client, "first")
    _create_pipe(client, "second")

    backend = pipes_backends[DEFAULT_ACCOUNT_ID][REGION]
    first = backend.pipes["first"]
    second = backend.pipes["second"]

    OrchestrationPlan.chain([first, second], target="RUNNING").execute()

    assert client.describe_pipe(Name="first")["CurrentState"] == "RUNNING"
    assert client.describe_pipe(Name="second")["CurrentState"] == "RUNNING"
    assert first.last_trigger == "orchestration"
    assert second.last_trigger == "orchestration"


@mock_aws
def test_orchestration_plan_failure_blocks_dependent():
    client = boto3.client("pipes", region_name=REGION)
    _create_pipe(client, "first")
    _create_pipe(client, "second")

    backend = pipes_backends[DEFAULT_ACCOUNT_ID][REGION]
    first = backend.pipes["first"]
    second = backend.pipes["second"]
    first.fail_at("CREATING", reason="boom")

    plan = OrchestrationPlan()
    plan.add(first, target="RUNNING")
    plan.add(second, target="RUNNING", depends_on=[first])

    with pytest.raises(OrchestrationError, match="did not complete"):
        plan.execute()

    assert client.describe_pipe(Name="first")["CurrentState"] == "CREATE_FAILED"
    # The dependent was never progressed
    assert second.status == "CREATING"

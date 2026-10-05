import boto3
import pytest
from botocore.exceptions import ClientError

from moto import mock_aws
from moto.core.references import (
    PolicyValue,
    ResourceCoordinate,
    delete_service_reference_policy,
    list_referrers,
    run_audit,
    set_service_reference_policy,
)
from tests.test_awslambda.utilities import get_test_zip_file1

REGION = "us-east-1"
ACCOUNT = "123456789012"


def _queue_coordinate() -> ResourceCoordinate:
    return ResourceCoordinate(
        service="sqs",
        account_id=ACCOUNT,
        region=REGION,
        resource_type="queue",
        resource_id="test-queue",
    )


def _setup() -> tuple[object, object, str, str]:
    iam = boto3.client("iam", region_name=REGION)
    role_arn = iam.create_role(RoleName="test-role", AssumeRolePolicyDocument="doc")[
        "Role"
    ]["Arn"]

    sqs_client = boto3.client("sqs", region_name=REGION)
    queue_url = sqs_client.create_queue(QueueName="test-queue")["QueueUrl"]
    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    lambda_client = boto3.client("lambda", region_name=REGION)
    lambda_client.create_function(
        FunctionName="test-fn",
        Runtime="python3.11",
        Role=role_arn,
        Handler="lambda_function.handler",
        Code={"ZipFile": get_test_zip_file1()},
    )
    esm = lambda_client.create_event_source_mapping(
        EventSourceArn=queue_arn, FunctionName="test-fn"
    )
    return lambda_client, sqs_client, queue_url, esm["UUID"]


@mock_aws
def test_event_source_mapping_edge_registered() -> None:
    _lambda_client, _sqs_client, _queue_url, esm_uuid = _setup()

    referrers = list_referrers(_queue_coordinate())
    assert len(referrers) == 1
    record = referrers[0]
    assert record.source.service == "lambda"
    assert record.source.account_id == ACCOUNT
    assert record.source.region == REGION
    assert record.source.resource_type == "event_source_mapping"
    assert record.source.resource_id == esm_uuid
    assert record.relation == "EventSourceMapping"


@mock_aws
def test_delete_event_source_mapping_removes_edge() -> None:
    lambda_client, _sqs_client, _queue_url, esm_uuid = _setup()
    assert len(list_referrers(_queue_coordinate())) == 1

    lambda_client.delete_event_source_mapping(UUID=esm_uuid)

    assert list_referrers(_queue_coordinate()) == []


@mock_aws
def test_deny_policy_blocks_queue_deletion() -> None:
    _lambda_client, sqs_client, queue_url, _esm_uuid = _setup()
    set_service_reference_policy("sqs", PolicyValue.DENY)

    with pytest.raises(ClientError) as exc_info:
        sqs_client.delete_queue(QueueUrl=queue_url)
    assert exc_info.value.response["Error"]["Code"] == "ReferenceViolation"

    # Queue still exists, mapping intact.
    assert sqs_client.get_queue_url(QueueName="test-queue")["QueueUrl"] == queue_url
    assert len(list_referrers(_queue_coordinate())) == 1

    delete_service_reference_policy("sqs")


@mock_aws
def test_cascade_deletes_mapping_then_queue() -> None:
    lambda_client, sqs_client, queue_url, esm_uuid = _setup()
    set_service_reference_policy("sqs", PolicyValue.CASCADE)

    sqs_client.delete_queue(QueueUrl=queue_url)

    # Mapping was cascaded away...
    mappings = lambda_client.list_event_source_mappings(FunctionName="test-fn")[
        "EventSourceMappings"
    ]
    assert all(mapping["UUID"] != esm_uuid for mapping in mappings)
    # ...and the queue no longer exists.
    with pytest.raises(ClientError):
        sqs_client.get_queue_url(QueueName="test-queue")
    assert list_referrers(_queue_coordinate()) == []

    delete_service_reference_policy("sqs")


@mock_aws
def test_audit_clean_after_mapping_flow() -> None:
    _setup()
    assert run_audit() == []

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

REGION = "us-east-1"
ACCOUNT = "123456789012"


def _queue_coordinate(name: str = "test-queue") -> ResourceCoordinate:
    return ResourceCoordinate(
        service="sqs",
        account_id=ACCOUNT,
        region=REGION,
        resource_type="queue",
        resource_id=name,
    )


def _setup_queue() -> tuple[object, str, str]:
    sqs = boto3.client("sqs", region_name=REGION)
    queue_url = sqs.create_queue(QueueName="test-queue")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    return sqs, queue_url, queue_arn


def _create_rule_with_sqs_target(
    target_id: str = "t1", queue_arn: str | None = None
) -> tuple[object, str, str, str]:
    events = boto3.client("events", region_name=REGION)
    _sqs_client, _queue_url, actual_queue_arn = _setup_queue()
    arn = queue_arn or actual_queue_arn
    events.put_rule(Name="test-rule", ScheduleExpression="rate(5 minutes)")
    events.put_targets(Rule="test-rule", Targets=[{"Id": target_id, "Arn": arn}])
    return events, actual_queue_arn, target_id, _queue_url  # type: ignore[return-value]


@mock_aws
def test_put_target_registers_edge() -> None:
    _events, _queue_arn, target_id, _queue_url = _create_rule_with_sqs_target()

    referrers = list_referrers(_queue_coordinate())
    assert len(referrers) == 1
    record = referrers[0]
    assert record.source.service == "events"
    assert record.source.account_id == ACCOUNT
    assert record.source.region == REGION
    assert record.source.resource_type == "rule_target"
    assert record.source.resource_id == f"default@test-rule@{target_id}"
    assert record.relation == "RuleTarget"


@mock_aws
def test_remove_target_removes_edge() -> None:
    events, _queue_arn, _target_id, _queue_url = _create_rule_with_sqs_target()
    assert len(list_referrers(_queue_coordinate())) == 1

    events.remove_targets(Rule="test-rule", Ids=["t1"])

    assert list_referrers(_queue_coordinate()) == []


@mock_aws
def test_partial_target_removal_keeps_other_edges() -> None:
    events, queue_arn, _target_id, _queue_url = _create_rule_with_sqs_target(
        target_id="t1"
    )
    # Second queue + second target
    sqs = boto3.client("sqs", region_name=REGION)
    second_url = sqs.create_queue(QueueName="second-queue")["QueueUrl"]
    second_arn = sqs.get_queue_attributes(
        QueueUrl=second_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    events.put_targets(Rule="test-rule", Targets=[{"Id": "t2", "Arn": second_arn}])
    assert len(list_referrers(_queue_coordinate())) == 1
    assert len(list_referrers(_queue_coordinate("second-queue"))) == 1

    events.remove_targets(Rule="test-rule", Ids=["t1"])

    assert list_referrers(_queue_coordinate()) == []
    assert len(list_referrers(_queue_coordinate("second-queue"))) == 1


@mock_aws
def test_delete_rule_removes_edges() -> None:
    events, _queue_arn, _target_id, _queue_url = _create_rule_with_sqs_target()
    assert len(list_referrers(_queue_coordinate())) == 1

    events.remove_targets(Rule="test-rule", Ids=["t1"])
    events.delete_rule(Name="test-rule")

    assert list_referrers(_queue_coordinate()) == []


@mock_aws
def test_target_replacement_moves_edge() -> None:
    events, _queue_arn, _target_id, _queue_url = _create_rule_with_sqs_target(
        target_id="t1"
    )
    sqs = boto3.client("sqs", region_name=REGION)
    new_url = sqs.create_queue(QueueName="new-queue")["QueueUrl"]
    new_arn = sqs.get_queue_attributes(QueueUrl=new_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]

    # Overwrite the same target id with a different queue ARN.
    events.put_targets(Rule="test-rule", Targets=[{"Id": "t1", "Arn": new_arn}])

    assert list_referrers(_queue_coordinate()) == []
    assert len(list_referrers(_queue_coordinate("new-queue"))) == 1


@mock_aws
def test_deny_policy_blocks_queue_deletion() -> None:
    _events, _queue_arn, _target_id, queue_url = _create_rule_with_sqs_target()
    sqs = boto3.client("sqs", region_name=REGION)
    set_service_reference_policy("sqs", PolicyValue.DENY)

    try:
        with pytest.raises(ClientError) as exc_info:
            sqs.delete_queue(QueueUrl=queue_url)
        assert exc_info.value.response["Error"]["Code"] == "ReferenceViolation"
        assert sqs.get_queue_url(QueueName="test-queue")["QueueUrl"] == queue_url
        assert len(list_referrers(_queue_coordinate())) == 1
    finally:
        delete_service_reference_policy("sqs")


@mock_aws
def test_cascade_deletes_target_then_queue() -> None:
    events, _queue_arn, target_id, queue_url = _create_rule_with_sqs_target()
    sqs = boto3.client("sqs", region_name=REGION)
    set_service_reference_policy("sqs", PolicyValue.CASCADE)

    sqs.delete_queue(QueueUrl=queue_url)

    # Rule target was cascaded away...
    targets = events.list_targets_by_rule(Rule="test-rule")["Targets"]
    assert all(target["Id"] != target_id for target in targets)
    # ...and the queue no longer exists.
    with pytest.raises(ClientError):
        sqs.get_queue_url(QueueName="test-queue")
    assert list_referrers(_queue_coordinate()) == []

    delete_service_reference_policy("sqs")


@mock_aws
def test_audit_clean_after_rule_target_flow() -> None:
    _create_rule_with_sqs_target()
    assert run_audit() == []

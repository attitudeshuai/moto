import boto3
import pytest
from botocore.exceptions import ClientError

from moto import mock_aws
from moto.core.references import (
    PolicyValue,
    ResourceCoordinate,
    delete_service_reference_policy,
    list_referrers,
    list_warnings,
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


def _setup() -> tuple[object, object, str, str, str]:
    sns = boto3.client("sns", region_name=REGION)
    sqs = boto3.client("sqs", region_name=REGION)
    queue_url = sqs.create_queue(QueueName="test-queue")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    topic_arn = sns.create_topic(Name="test-topic")["TopicArn"]
    sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)[
        "SubscriptionArn"
    ]
    return sns, sqs, queue_url, topic_arn, sub_arn


@mock_aws
def test_subscription_edge_registered_and_reverse_lookup() -> None:
    _sns, _sqs, queue_url, topic_arn, sub_arn = _setup()

    referrers = list_referrers(_queue_coordinate())
    assert len(referrers) == 1
    record = referrers[0]
    assert record.source.service == "sns"
    assert record.source.account_id == ACCOUNT
    assert record.source.region == REGION
    assert record.source.resource_type == "subscription"
    assert record.source.resource_id == sub_arn
    assert record.relation == "Subscription"


@mock_aws
def test_unsubscribe_removes_edge() -> None:
    sns, _sqs, _queue_url, _topic_arn, sub_arn = _setup()
    assert len(list_referrers(_queue_coordinate())) == 1

    sns.unsubscribe(SubscriptionArn=sub_arn)

    assert list_referrers(_queue_coordinate()) == []


@mock_aws
def test_delete_topic_removes_subscription_edges() -> None:
    sns, _sqs, _queue_url, topic_arn, _sub_arn = _setup()
    assert len(list_referrers(_queue_coordinate())) == 1

    sns.delete_topic(TopicArn=topic_arn)

    assert list_referrers(_queue_coordinate()) == []


@mock_aws
def test_deny_policy_blocks_queue_deletion() -> None:
    _sns, sqs, queue_url, _topic_arn, _sub_arn = _setup()
    set_service_reference_policy("sqs", PolicyValue.DENY)

    try:
        with pytest.raises(ClientError) as exc_info:
            sqs.delete_queue(QueueUrl=queue_url)
        assert exc_info.value.response["Error"]["Code"] == "ReferenceViolation"

        # Queue still exists, subscription still intact.
        assert sqs.get_queue_url(QueueName="test-queue")["QueueUrl"] == queue_url
        referrers = list_referrers(_queue_coordinate())
        assert len(referrers) == 1
    finally:
        delete_service_reference_policy("sqs")


@mock_aws
def test_warn_policy_allows_deletion_and_audits_missing_target() -> None:
    _sns, sqs, queue_url, _topic_arn, _sub_arn = _setup()
    set_service_reference_policy("sqs", PolicyValue.WARN)

    sqs.delete_queue(QueueUrl=queue_url)

    warnings = list_warnings(_queue_coordinate())
    assert len(warnings) == 1
    assert warnings[0].operation == "delete"
    assert len(warnings[0].references) == 1

    findings = run_audit()
    missing_targets = [f for f in findings if f.type == "missing_target"]
    assert len(missing_targets) == 1
    assert missing_targets[0].target == _queue_coordinate()

    delete_service_reference_policy("sqs")


@mock_aws
def test_cascade_deletes_subscription_then_queue() -> None:
    sns, sqs, queue_url, topic_arn, sub_arn = _setup()
    set_service_reference_policy("sqs", PolicyValue.CASCADE)

    sqs.delete_queue(QueueUrl=queue_url)

    # Subscription was cascaded away...
    subscriptions = sns.list_subscriptions()["Subscriptions"]
    assert all(
        subscription["SubscriptionArn"] != sub_arn for subscription in subscriptions
    )
    # ...and the queue no longer exists.
    with pytest.raises(ClientError):
        sqs.get_queue_url(QueueName="test-queue")
    assert list_referrers(_queue_coordinate()) == []

    delete_service_reference_policy("sqs")


@mock_aws
def test_audit_clean_after_regular_subscription_flow() -> None:
    _setup()
    assert run_audit() == []

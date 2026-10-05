"""Tests for the reliable S3 notification delivery pathway.

Covers queryable delivery records, configurable backoff retries, dead-letter
routing, undeliverable markers, explicit switches, concurrency safety,
convergence when targets disappear mid-delivery and record capacity eviction.
"""

import json
import threading
import time

import boto3
import pytest

from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID as ACCOUNT_ID
from moto.s3.delivery import (
    TERMINAL_STATUSES,
    BackoffStrategy,
    DeliveryManager,
    DeliveryStatus,
    RetryPolicy,
)
from moto.s3.models import s3_backends

REGION = "us-east-1"


def queue_arn(name: str) -> str:
    return f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:{name}"


def topic_arn(name: str) -> str:
    return f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:{name}"


def lambda_arn(name: str) -> str:
    return f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{name}"


@pytest.fixture(name="clients")
def fixture_clients():
    with mock_aws():
        yield {
            "s3": boto3.client("s3", region_name=REGION),
            "sqs": boto3.client("sqs", region_name=REGION),
            "sns": boto3.client("sns", region_name=REGION),
            "events": boto3.client("events", region_name=REGION),
        }


@pytest.fixture(name="manager")
def fixture_manager():
    # S3 backends are partition scoped.
    return s3_backends[ACCOUNT_ID]["aws"].delivery_manager


def make_bucket(s3_client, name: str) -> str:
    s3_client.create_bucket(Bucket=name)
    return f"arn:aws:s3:::{name}"


def make_queue(sqs_client, name: str) -> tuple[str, str]:
    url = sqs_client.create_queue(QueueName=name)["QueueUrl"]
    return url, queue_arn(name)


def configure_queue_notification(
    s3_client, bucket: str, arn: str, config_id: str = "notif-id"
) -> None:
    s3_client.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "Id": config_id,
                    "QueueArn": arn,
                    "Events": ["s3:ObjectCreated:*"],
                }
            ]
        },
    )


def receive_bodies(sqs_client, queue_url: str) -> list[dict]:
    messages = sqs_client.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=0
    ).get("Messages", [])
    return [json.loads(message["Body"]) for message in messages]


def wait_for_status(manager, target_arn: str, expected: str, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        records = manager.list_records(target_arn=target_arn)
        if records and records[0]["status"] == expected:
            return records[0]
        time.sleep(0.01)
    raise AssertionError(
        f"Record for {target_arn} never reached {expected}: "
        f"{manager.list_records(target_arn=target_arn)}"
    )


# ---------------------------------------------------------------------------
# Records & default behaviour
# ---------------------------------------------------------------------------
@mock_aws
def test_successful_delivery_is_recorded(clients, manager):
    s3, sqs = clients["s3"], clients["sqs"]
    bucket_arn = make_bucket(s3, "bucket-a")
    queue_url, arn = make_queue(sqs, "queue-a")
    configure_queue_notification(s3, "bucket-a", arn)
    sqs.purge_queue(QueueUrl=queue_url)  # drop the s3:TestEvent

    assert manager.list_records() == []

    s3.put_object(Bucket="bucket-a", Key="key-1", Body=b"hello")

    records = manager.list_records()
    assert len(records) == 1
    record = records[0]
    assert record["status"] == DeliveryStatus.SUCCEEDED.value
    assert record["attempts"] == 1
    assert record["max_attempts"] == 1
    assert record["source_arn"] == bucket_arn
    assert record["target_arn"] == arn
    assert record["target_type"] == "sqs"
    assert record["configuration_id"] == "notif-id"
    assert record["last_failure_reason"] is None
    assert record["source"]["bucket"] == "bucket-a"
    assert record["source"]["event"] == "s3:ObjectCreated:Put"
    assert record["source"]["key"] == "key-1"

    bodies = receive_bodies(sqs, queue_url)
    assert len(bodies) == 1
    assert bodies[0]["Records"][0]["s3"]["object"]["key"] == "key-1"

    # Filters
    assert len(manager.list_records(source_arn=bucket_arn)) == 1
    assert len(manager.list_records(target_arn=arn)) == 1
    assert len(manager.list_records(status=DeliveryStatus.SUCCEEDED.value)) == 1
    assert manager.list_records(target_arn=queue_arn("other")) == []
    assert manager.get_record(record["id"])["status"] == "SUCCEEDED"


@mock_aws
def test_missing_target_is_undelivered_without_raising(clients, manager):
    s3 = clients["s3"]
    make_bucket(s3, "bucket-b")
    configure_queue_notification(s3, "bucket-b", queue_arn("ghost-queue"))

    # The object operation must succeed even though the target does not exist.
    s3.put_object(Bucket="bucket-b", Key="key-1", Body=b"hi")

    records = manager.list_records()
    assert len(records) == 1
    record = records[0]
    assert record["status"] == DeliveryStatus.UNDELIVERED.value
    assert record["attempts"] == 1
    assert "QueueDoesNotExist" in record["last_failure_reason"]
    assert len(record["attempt_history"]) == 1


@mock_aws
def test_default_behaviour_single_inline_attempt(clients, manager):
    s3 = clients["s3"]
    make_bucket(s3, "bucket-c")
    configure_queue_notification(s3, "bucket-c", queue_arn("ghost-2"))

    start = time.time()
    s3.put_object(Bucket="bucket-c", Key="k", Body=b"x")
    # No retry scheduling: the call returns immediately with a verdict.
    assert time.time() - start < 1

    record = manager.list_records()[0]
    assert record["status"] == DeliveryStatus.UNDELIVERED.value
    assert record["attempts"] == 1


# ---------------------------------------------------------------------------
# Retries
# ---------------------------------------------------------------------------
@mock_aws
def test_retry_succeeds_once_target_appears(clients, manager):
    s3, sqs = clients["s3"], clients["sqs"]
    make_bucket(s3, "bucket-d")
    arn = queue_arn("late-queue")
    configure_queue_notification(s3, "bucket-d", arn)
    manager.enable_retries(
        max_attempts=20, backoff=BackoffStrategy.FIXED, base_delay=0.02
    )

    s3.put_object(Bucket="bucket-d", Key="late", Body=b"x")
    record = manager.list_records(target_arn=arn)[0]
    assert record["status"] == DeliveryStatus.RETRYING.value
    assert record["attempts"] == 1
    assert record["next_attempt_at"] is not None

    # The target comes into existence a moment later.
    queue_url, _ = make_queue(sqs, "late-queue")

    assert manager.wait_idle(timeout=5)
    record = manager.list_records(target_arn=arn)[0]
    assert record["status"] == DeliveryStatus.SUCCEEDED.value
    assert record["attempts"] >= 2
    assert len(receive_bodies(sqs, queue_url)) == 1


@mock_aws
def test_retries_exhausted_is_undelivered(clients, manager):
    s3 = clients["s3"]
    make_bucket(s3, "bucket-e")
    arn = queue_arn("never-queue")
    configure_queue_notification(s3, "bucket-e", arn)
    manager.enable_retries(
        max_attempts=3, backoff=BackoffStrategy.FIXED, base_delay=0.01
    )

    s3.put_object(Bucket="bucket-e", Key="k", Body=b"x")
    assert manager.wait_idle(timeout=5)

    record = manager.list_records(target_arn=arn)[0]
    assert record["status"] == DeliveryStatus.UNDELIVERED.value
    assert record["attempts"] == 3
    assert record["max_attempts"] == 3
    assert record["next_attempt_at"] is None
    assert [entry["attempt"] for entry in record["attempt_history"]] == [1, 2, 3]


@mock_aws
def test_disabling_retries_restores_single_attempt(clients, manager):
    s3 = clients["s3"]
    make_bucket(s3, "bucket-f")
    arn = queue_arn("disabled-retry-queue")
    configure_queue_notification(s3, "bucket-f", arn)
    manager.enable_retries(
        max_attempts=5, backoff=BackoffStrategy.FIXED, base_delay=0.01
    )
    manager.disable_retries()

    s3.put_object(Bucket="bucket-f", Key="k", Body=b"x")
    assert manager.wait_idle(timeout=2)
    record = manager.list_records(target_arn=arn)[0]
    assert record["attempts"] == 1
    assert record["status"] == DeliveryStatus.UNDELIVERED.value


# ---------------------------------------------------------------------------
# Dead-letter queue
# ---------------------------------------------------------------------------
@mock_aws
def test_exhausted_delivery_moves_to_dead_letter_queue(clients, manager):
    s3, sqs = clients["s3"], clients["sqs"]
    make_bucket(s3, "bucket-g")
    target_arn = queue_arn("missing-target")
    dlq_url, dlq_arn = make_queue(sqs, "dlq")
    configure_queue_notification(s3, "bucket-g", target_arn)
    manager.configure(
        retries_enabled=True,
        max_attempts=2,
        backoff=BackoffStrategy.FIXED,
        base_delay=0.01,
        dead_letter_enabled=True,
    )
    manager.register_dead_letter_queue(target_arn, dlq_arn)

    s3.put_object(Bucket="bucket-g", Key="dead", Body=b"x")
    assert manager.wait_idle(timeout=5)

    record = manager.list_records(target_arn=target_arn)[0]
    assert record["status"] == DeliveryStatus.DEAD_LETTERED.value
    assert record["attempts"] == 2
    assert record["dead_letter_queue_arn"] == dlq_arn

    bodies = receive_bodies(sqs, dlq_url)
    assert len(bodies) == 1
    envelope = bodies[0]
    assert envelope["targetArn"] == target_arn
    assert envelope["targetType"] == "sqs"
    assert envelope["attempts"] == 2
    assert "QueueDoesNotExist" in envelope["failureReason"]
    assert envelope["event"]["Records"][0]["s3"]["bucket"]["name"] == "bucket-g"


@mock_aws
def test_dlq_enabled_without_registration_is_undelivered(clients, manager):
    s3 = clients["s3"]
    make_bucket(s3, "bucket-h")
    target_arn = queue_arn("missing-no-dlq")
    configure_queue_notification(s3, "bucket-h", target_arn)
    manager.configure(
        retries_enabled=True,
        max_attempts=2,
        base_delay=0.01,
        dead_letter_enabled=True,
    )

    s3.put_object(Bucket="bucket-h", Key="k", Body=b"x")
    assert manager.wait_idle(timeout=5)
    record = manager.list_records(target_arn=target_arn)[0]
    assert record["status"] == DeliveryStatus.UNDELIVERED.value
    assert record["dead_letter_queue_arn"] is None


@mock_aws
def test_retry_off_dlq_on_moves_after_single_attempt(clients, manager):
    s3, sqs = clients["s3"], clients["sqs"]
    make_bucket(s3, "bucket-i")
    target_arn = queue_arn("missing-direct-dlq")
    dlq_url, dlq_arn = make_queue(sqs, "direct-dlq")
    configure_queue_notification(s3, "bucket-i", target_arn)
    manager.enable_dead_letter()
    manager.register_dead_letter_queue(target_arn, dlq_arn)

    s3.put_object(Bucket="bucket-i", Key="k", Body=b"x")
    assert manager.wait_idle(timeout=5)
    record = manager.list_records(target_arn=target_arn)[0]
    assert record["attempts"] == 1
    assert record["status"] == DeliveryStatus.DEAD_LETTERED.value
    assert len(receive_bodies(sqs, dlq_url)) == 1


@mock_aws
def test_disabling_dead_letter_marks_undelivered(clients, manager):
    s3, sqs = clients["s3"], clients["sqs"]
    make_bucket(s3, "bucket-j")
    target_arn = queue_arn("missing-dlq-disabled")
    _, dlq_arn = make_queue(sqs, "unused-dlq")
    configure_queue_notification(s3, "bucket-j", target_arn)
    manager.configure(retries_enabled=True, max_attempts=2, base_delay=0.01)
    manager.enable_dead_letter()
    manager.register_dead_letter_queue(target_arn, dlq_arn)
    manager.disable_dead_letter()

    s3.put_object(Bucket="bucket-j", Key="k", Body=b"x")
    assert manager.wait_idle(timeout=5)
    record = manager.list_records(target_arn=target_arn)[0]
    assert record["status"] == DeliveryStatus.UNDELIVERED.value
    assert record["dead_letter_queue_arn"] is None


@mock_aws
def test_native_queue_redrive_policy_is_used_as_dlq(clients, manager):
    s3, sqs = clients["s3"], clients["sqs"]
    make_bucket(s3, "bucket-native")
    dlq_url, dlq_arn = make_queue(sqs, "native-dlq")
    # The target queue exists and carries its own redrive policy; sends fail
    # transiently and the native DLQ registration should be picked up.
    target_url, target_arn = make_queue(sqs, "native-target")
    sqs.set_queue_attributes(
        QueueUrl=target_url,
        Attributes={
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "1"}
            )
        },
    )
    configure_queue_notification(s3, "bucket-native", target_arn)
    manager.configure(retries_enabled=True, max_attempts=2, base_delay=0.01)
    manager.enable_dead_letter()

    backend, original = _patch_sqs_send_to_fail(sqs, "native-target")
    try:
        s3.put_object(Bucket="bucket-native", Key="k", Body=b"x")
        assert manager.wait_idle(timeout=5)
        record = manager.list_records(target_arn=target_arn)[0]
        assert record["status"] == DeliveryStatus.DEAD_LETTERED.value
        assert record["dead_letter_queue_arn"] == dlq_arn
        assert len(receive_bodies(sqs, dlq_url)) == 1
    finally:
        backend.send_message = original


# ---------------------------------------------------------------------------
# Convergence
# ---------------------------------------------------------------------------
def _patch_sqs_send_to_fail(sqs_client, failing_queue_name: str):
    from moto.sqs.models import sqs_backends

    backend = sqs_backends[ACCOUNT_ID][REGION]
    original = backend.send_message

    def fail(*, queue_name: str, **kwargs):
        if queue_name == failing_queue_name:
            raise RuntimeError("transient delivery failure")
        return original(queue_name=queue_name, **kwargs)

    backend.send_message = fail
    return backend, original


@mock_aws
def test_deleting_target_converges_inflight_retries(clients, manager):
    s3, sqs = clients["s3"], clients["sqs"]
    make_bucket(s3, "bucket-k")
    queue_url, arn = make_queue(sqs, "doomed-queue")
    configure_queue_notification(s3, "bucket-k", arn)
    manager.enable_retries(
        max_attempts=100, backoff=BackoffStrategy.FIXED, base_delay=0.02
    )

    backend, original = _patch_sqs_send_to_fail(sqs, "doomed-queue")
    try:
        s3.put_object(Bucket="bucket-k", Key="k", Body=b"x")
        wait_for_status(manager, arn, DeliveryStatus.RETRYING.value)
        attempts_at_cancel = manager.list_records(target_arn=arn)[0]["attempts"]

        # Deleting the target must cancel every pending/in-flight retry.
        sqs.delete_queue(QueueUrl=queue_url)
        assert manager.wait_idle(timeout=5)

        record = manager.list_records(target_arn=arn)[0]
        assert record["status"] == DeliveryStatus.CANCELLED.value
        attempts_after = record["attempts"]
        assert attempts_after >= attempts_at_cancel

        # No further attempts happen after convergence.
        time.sleep(0.1)
        assert manager.list_records(target_arn=arn)[0]["attempts"] == attempts_after
    finally:
        backend.send_message = original


@mock_aws
def test_removing_notification_configuration_converges_retries(clients, manager):
    s3, sqs = clients["s3"], clients["sqs"]
    make_bucket(s3, "bucket-l")
    arn = queue_arn("reconfigured-away")
    configure_queue_notification(s3, "bucket-l", arn)
    manager.enable_retries(
        max_attempts=100, backoff=BackoffStrategy.FIXED, base_delay=0.02
    )

    backend, original = _patch_sqs_send_to_fail(sqs, "reconfigured-away")
    try:
        s3.put_object(Bucket="bucket-l", Key="k", Body=b"x")
        wait_for_status(manager, arn, DeliveryStatus.RETRYING.value)

        s3.put_bucket_notification_configuration(
            Bucket="bucket-l", NotificationConfiguration={}
        )
        assert manager.wait_idle(timeout=5)
        assert (
            manager.list_records(target_arn=arn)[0]["status"]
            == DeliveryStatus.CANCELLED.value
        )
    finally:
        backend.send_message = original


@mock_aws
def test_deleting_bucket_converges_retries(clients, manager):
    s3 = clients["s3"]
    bucket_arn = make_bucket(s3, "bucket-m")
    arn = queue_arn("bucket-gone-queue")
    configure_queue_notification(s3, "bucket-m", arn)
    manager.enable_retries(
        max_attempts=100, backoff=BackoffStrategy.FIXED, base_delay=0.02
    )

    s3.put_object(Bucket="bucket-m", Key="k", Body=b"x")
    wait_for_status(manager, arn, DeliveryStatus.RETRYING.value)

    s3.delete_object(Bucket="bucket-m", Key="k")
    s3.delete_bucket(Bucket="bucket-m")
    assert manager.wait_idle(timeout=5)
    records = manager.list_records(source_arn=bucket_arn)
    assert records
    assert all(record["status"] == DeliveryStatus.CANCELLED.value for record in records)


# ---------------------------------------------------------------------------
# Capacity / eviction / filtering
# ---------------------------------------------------------------------------
@mock_aws
def test_record_capacity_evicts_oldest(clients, manager):
    s3, sqs = clients["s3"], clients["sqs"]
    make_bucket(s3, "bucket-n")
    queue_url, arn = make_queue(sqs, "capacity-queue")
    configure_queue_notification(s3, "bucket-n", arn)
    sqs.purge_queue(QueueUrl=queue_url)
    manager.configure(max_records=5)

    for index in range(8):
        s3.put_object(Bucket="bucket-n", Key=f"key-{index}", Body=b"x")

    records = manager.list_records(target_arn=arn)
    assert len(records) == 5
    assert [record["source"]["key"] for record in records] == [
        f"key-{index}" for index in range(3, 8)
    ]


@mock_aws
def test_records_filter_by_source_target_and_type(clients, manager):
    s3, sqs = clients["s3"], clients["sqs"]
    arn_a = make_bucket(s3, "bucket-o")
    arn_b = make_bucket(s3, "bucket-p")
    _, q1 = make_queue(sqs, "q-1")
    _, q2 = make_queue(sqs, "q-2")
    configure_queue_notification(s3, "bucket-o", q1)
    configure_queue_notification(s3, "bucket-p", q2)

    s3.put_object(Bucket="bucket-o", Key="a", Body=b"x")
    s3.put_object(Bucket="bucket-p", Key="b", Body=b"x")

    assert len(manager.list_records()) == 2
    assert len(manager.list_records(source_arn=arn_a)) == 1
    assert len(manager.list_records(source_arn=arn_b)) == 1
    assert len(manager.list_records(target_arn=q1)) == 1
    assert len(manager.list_records(target_type="sqs")) == 2
    assert len(manager.list_records(target_type="sns")) == 0


# ---------------------------------------------------------------------------
# Other target types
# ---------------------------------------------------------------------------
@mock_aws
def test_sns_targets_are_recorded(clients, manager):
    s3, sns = clients["s3"], clients["sns"]
    make_bucket(s3, "bucket-sns")
    topic = sns.create_topic(Name="sns-topic")["TopicArn"]
    s3.put_bucket_notification_configuration(
        Bucket="bucket-sns",
        NotificationConfiguration={
            "TopicConfigurations": [
                {"Id": "t", "TopicArn": topic, "Events": ["s3:ObjectCreated:*"]}
            ]
        },
    )
    s3.put_object(Bucket="bucket-sns", Key="k", Body=b"x")
    record = manager.list_records(target_arn=topic)[0]
    assert record["target_type"] == "sns"
    assert record["status"] == DeliveryStatus.SUCCEEDED.value

    missing = topic_arn("missing-topic")
    s3.put_bucket_notification_configuration(
        Bucket="bucket-sns",
        NotificationConfiguration={
            "TopicConfigurations": [
                {"Id": "t2", "TopicArn": missing, "Events": ["s3:ObjectCreated:*"]}
            ]
        },
    )
    s3.put_object(Bucket="bucket-sns", Key="k2", Body=b"x")
    record = manager.list_records(target_arn=missing)[0]
    assert record["status"] == DeliveryStatus.UNDELIVERED.value


@mock_aws
def test_missing_lambda_target_is_undelivered(clients, manager):
    s3 = clients["s3"]
    make_bucket(s3, "bucket-lambda")
    arn = lambda_arn("missing-function")
    s3.put_bucket_notification_configuration(
        Bucket="bucket-lambda",
        NotificationConfiguration={
            "LambdaFunctionConfigurations": [
                {
                    "Id": "lf",
                    "LambdaFunctionArn": arn,
                    "Events": ["s3:ObjectCreated:*"],
                }
            ]
        },
    )
    s3.put_object(Bucket="bucket-lambda", Key="k", Body=b"x")
    record = manager.list_records(target_arn=arn)[0]
    assert record["target_type"] == "lambda"
    assert record["status"] == DeliveryStatus.UNDELIVERED.value
    assert record["attempts"] == 1


@mock_aws
def test_eventbridge_target_failures_are_isolated(clients, manager):
    s3, sqs, events = clients["s3"], clients["sqs"], clients["events"]
    make_bucket(s3, "bucket-eb")
    queue_url, q_arn = make_queue(sqs, "eb-target-queue")
    events.put_rule(
        Name="eb-rule",
        EventPattern=json.dumps({"source": ["aws.s3"]}),
    )
    bad_arn = lambda_arn("unsupported-eb-target")
    events.put_targets(
        Rule="eb-rule",
        Targets=[
            {"Id": "good", "Arn": q_arn},
            {"Id": "bad", "Arn": bad_arn},
        ],
    )
    s3.put_bucket_notification_configuration(
        Bucket="bucket-eb",
        NotificationConfiguration={"EventBridgeConfiguration": {}},
    )
    sqs.purge_queue(QueueUrl=queue_url)

    s3.put_object(Bucket="bucket-eb", Key="k", Body=b"x")

    # Moto emits an ObjectCreated plus an ObjectTagging event per put; every
    # event fans out to both targets independently.
    good = manager.list_records(target_arn=q_arn)
    bad = manager.list_records(target_arn=bad_arn)
    assert good
    assert len(bad) == len(good)
    assert all(record["target_type"] == "eventbridge" for record in good)
    assert all(record["status"] == DeliveryStatus.SUCCEEDED.value for record in good)
    # Lambda targets are not implemented by moto's EventBridge mock: the
    # failures are recorded but isolated from the healthy target.
    assert all(record["status"] == DeliveryStatus.UNDELIVERED.value for record in bad)
    assert all("NotImplementedError" in record["last_failure_reason"] for record in bad)
    assert len(receive_bodies(sqs, queue_url)) == len(good)


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------
@mock_aws
def test_concurrent_deliveries_have_consistent_terminal_states(clients, manager):
    s3, sqs = clients["s3"], clients["sqs"]
    good_bucket = make_bucket(s3, "bucket-good")
    bad_bucket = make_bucket(s3, "bucket-bad")
    good_url, good_arn = make_queue(sqs, "good-queue")
    bad_arn = queue_arn("bad-queue")
    configure_queue_notification(s3, "bucket-good", good_arn)
    configure_queue_notification(s3, "bucket-bad", bad_arn)
    sqs.purge_queue(QueueUrl=good_url)
    manager.enable_retries(
        max_attempts=2, backoff=BackoffStrategy.FIXED, base_delay=0.01
    )

    def producer(bucket: str, count: int) -> None:
        for index in range(count):
            clients["s3"].put_object(
                Bucket=bucket, Key=f"k-{threading.get_ident()}-{index}", Body=b"x"
            )

    threads = [
        threading.Thread(target=producer, args=("bucket-good", 4)),
        threading.Thread(target=producer, args=("bucket-bad", 4)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert manager.wait_idle(timeout=10)
    records = manager.list_records()
    assert len(records) == 8
    for record in records:
        assert record["status"] in {s.value for s in TERMINAL_STATUSES}
        assert record["attempts"] <= 2

    good_records = manager.list_records(source_arn=good_bucket)
    bad_records = manager.list_records(source_arn=bad_bucket)
    assert all(r["status"] == DeliveryStatus.SUCCEEDED.value for r in good_records)
    assert all(r["status"] == DeliveryStatus.UNDELIVERED.value for r in bad_records)
    assert all(r["attempts"] == 2 for r in bad_records)
    assert len(receive_bodies(sqs, good_url)) == 4


# ---------------------------------------------------------------------------
# Backoff & manager unit behaviour (deterministic fake timers)
# ---------------------------------------------------------------------------
class FakeTimer:
    def __init__(self, delay, function, args=()):
        self.delay = delay
        self.function = function
        self.args = args
        self.cancelled = False

    def start(self) -> None:
        pass

    def cancel(self) -> None:
        self.cancelled = True

    def fire(self) -> None:
        if not self.cancelled:
            self.function(*self.args)


def test_backoff_delays():
    fixed = RetryPolicy(
        enabled=True,
        max_attempts=4,
        backoff=BackoffStrategy.FIXED,
        base_delay=2,
        max_delay=100,
        multiplier=2,
    )
    assert [fixed.delay_for(n) for n in (1, 2, 3)] == [2, 2, 2]

    linear = RetryPolicy(
        enabled=True,
        max_attempts=4,
        backoff=BackoffStrategy.LINEAR,
        base_delay=1,
        max_delay=100,
    )
    assert [linear.delay_for(n) for n in (1, 2, 3)] == [1, 2, 3]

    exponential = RetryPolicy(
        enabled=True,
        max_attempts=4,
        backoff=BackoffStrategy.EXPONENTIAL,
        base_delay=1,
        max_delay=100,
        multiplier=2,
    )
    assert [exponential.delay_for(n) for n in (1, 2, 3)] == [1, 2, 4]

    capped = RetryPolicy(
        enabled=True, max_attempts=4, base_delay=10, max_delay=15, multiplier=2
    )
    assert capped.delay_for(3) == 15


def test_retry_pathway_with_fake_timers_succeeds_on_third_attempt():
    manager = DeliveryManager()
    manager._timer_factory = FakeTimer
    manager.enable_retries(
        max_attempts=3, backoff=BackoffStrategy.FIXED, base_delay=0.5
    )

    attempts = {"count": 0}

    def flaky():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("not yet")

    record = manager.submit(
        source_arn="arn:aws:s3:::b",
        target_arn="arn:aws:sqs:us-east-1:1:q",
        target_type="sqs",
        payload={"event": True},
        attempt=flaky,
    )
    assert record.status == DeliveryStatus.RETRYING
    assert record.attempts == 1

    record._timer.fire()
    assert manager.get_record(record.id)["attempts"] == 2
    assert manager.get_record(record.id)["status"] == "RETRYING"

    record._timer.fire()
    final = manager.get_record(record.id)
    assert final["status"] == "SUCCEEDED"
    assert final["attempts"] == 3


def test_retry_pathway_cancel_converges_scheduled_retry():
    manager = DeliveryManager()
    manager._timer_factory = FakeTimer
    manager.enable_retries(max_attempts=5, base_delay=1)

    record = manager.submit(
        source_arn="arn:aws:s3:::b",
        target_arn="arn:aws:sqs:us-east-1:1:q",
        target_type="sqs",
        payload=None,
        attempt=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert record.status == DeliveryStatus.RETRYING
    scheduled_timer = record._timer

    assert manager.cancel_target("arn:aws:sqs:us-east-1:1:q") == 1
    assert manager.get_record(record.id)["status"] == "CANCELLED"

    # A late timer fire must not override the terminal state.
    scheduled_timer.fire()
    assert manager.get_record(record.id)["status"] == "CANCELLED"


def test_invalid_configuration_rejected():
    manager = DeliveryManager()
    with pytest.raises(ValueError):
        manager.configure(max_attempts=0)
    with pytest.raises(ValueError):
        manager.register_dead_letter_queue("arn:aws:sqs:us-east-1:1:q", "not-an-arn")


@mock_aws
def test_backend_reset_shuts_down_pending_retries(clients, manager):
    s3 = clients["s3"]
    make_bucket(s3, "bucket-reset")
    arn = queue_arn("reset-queue")
    configure_queue_notification(s3, "bucket-reset", arn)
    manager.enable_retries(
        max_attempts=100, backoff=BackoffStrategy.FIXED, base_delay=0.02
    )
    s3.put_object(Bucket="bucket-reset", Key="k", Body=b"x")
    wait_for_status(manager, arn, DeliveryStatus.RETRYING.value)

    backend = s3_backends[ACCOUNT_ID]["aws"]
    backend.reset()

    new_manager = backend.delivery_manager
    assert new_manager.list_records() == []
    assert new_manager.wait_idle(timeout=1)
    # Pending retries of the old, shut down manager do not resurrect.
    time.sleep(0.1)
    assert new_manager.list_records() == []

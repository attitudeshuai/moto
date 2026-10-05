import threading
import time
from datetime import datetime
from typing import Any
from uuid import uuid4

import boto3
import pytest
from botocore.exceptions import ClientError

from moto import mock_aws, settings
from moto.core import DEFAULT_ACCOUNT_ID as ACCOUNT_ID
from moto.core import (
    DecisionEffect,
    DecisionSource,
    DenyCategory,
    add_auth_failure_injection,
    clear_auth_failure_injections,
    configure_auth_decisions,
    dropped_auth_decisions,
    enable_iam_authentication,
    get_auth_decision_log,
    get_auth_decisions,
    inject_auth_failure,
    remove_auth_failure_injection,
    reset_auth_decisions,
)
from moto.core.auth_decision_log import AuthorizationEvaluation

from .test_auth import (
    create_user_with_access_key,
    create_user_with_access_key_and_attached_policy,
    create_user_with_access_key_and_inline_policy,
)

USER_ARN = f"arn:aws:iam::{ACCOUNT_ID}:user/test-user"
SQS_QUEUE_ARN = f"arn:aws:sqs:us-east-1:{ACCOUNT_ID}:{{name}}"


@pytest.fixture(autouse=True)
def _clean_decision_log() -> None:
    if not settings.TEST_DECORATOR_MODE:
        pytest.skip("Auth decision log tests only run in decorator mode")
    log = get_auth_decision_log()
    log.reset_all()
    log.configure(max_records=log.DEFAULT_MAX_RECORDS)
    yield
    log.reset_all()
    log.configure(max_records=log.DEFAULT_MAX_RECORDS)


def _policy(*statements: dict) -> dict:
    return {"Version": "2012-10-17", "Statement": list(statements)}


def _client(
    service: str,
    access_key: dict[str, str],
    *,
    secret_access_key: str | None = None,
) -> Any:
    return boto3.client(
        service,
        region_name="us-east-1",
        aws_access_key_id=access_key["AccessKeyId"],
        aws_secret_access_key=secret_access_key or access_key["SecretAccessKey"],
    )


# ---------------------------------------------------------------------------
# Auth disabled: nothing recorded, nothing injected
# ---------------------------------------------------------------------------
@mock_aws
def test_nothing_recorded_or_injected_when_authentication_disabled() -> None:
    client = boto3.client("sqs", region_name="us-east-1")
    queue_url = client.create_queue(QueueName="default-off")["QueueUrl"]
    client.send_message(QueueUrl=queue_url, MessageBody="hello")

    assert get_auth_decisions() == ()
    assert dropped_auth_decisions() == 0

    rule = add_auth_failure_injection(actions=["sqs:SendMessage"])
    try:
        # Injection is inert while authentication is disabled
        client.send_message(QueueUrl=queue_url, MessageBody="hello")
    finally:
        remove_auth_failure_injection(rule)

    assert get_auth_decisions() == ()


# ---------------------------------------------------------------------------
# Observation of real decisions
# ---------------------------------------------------------------------------
@mock_aws
def test_allow_decision_is_recorded_with_full_context() -> None:
    access_key = create_user_with_access_key_and_inline_policy(
        "test-user", _policy({"Effect": "Allow", "Action": "sqs:*", "Resource": "*"})
    )
    client = _client("sqs", access_key)

    with enable_iam_authentication():
        queue_url = client.create_queue(QueueName="allowed-queue")["QueueUrl"]
        client.send_message(QueueUrl=queue_url, MessageBody="hello")

    decisions = get_auth_decisions()
    assert [d.action for d in decisions] == ["sqs:CreateQueue", "sqs:SendMessage"]

    decision = decisions[-1]
    assert decision.effect == DecisionEffect.ALLOW
    assert decision.source == DecisionSource.POLICY
    assert decision.injected is False
    assert decision.deny_category is None
    assert decision.deny_reason is None
    assert decision.account_id == ACCOUNT_ID
    assert decision.region == "us-east-1"
    assert decision.service == "sqs"
    assert decision.principal == USER_ARN
    assert decision.resource == SQS_QUEUE_ARN.format(name="allowed-queue")
    assert isinstance(decision.request_id, str) and len(decision.request_id) == 32
    assert isinstance(decision.timestamp, datetime)

    policy_decision = decision.policy_decisions[0]
    assert policy_decision.policy_id == "policy1"
    assert policy_decision.kind == "identity"
    assert policy_decision.result == "PERMITTED"
    matched = next(s for s in policy_decision.statements if s.action_matched)
    assert matched.effect == "Allow"
    assert matched.resource_matched is True
    assert matched.matched_resource_pattern == "*"
    assert matched.result == "PERMITTED"


@mock_aws
def test_attached_policy_arn_is_recorded_as_policy_id() -> None:
    access_key = create_user_with_access_key_and_attached_policy(
        "test-user",
        _policy({"Effect": "Allow", "Action": "sqs:ListQueues", "Resource": "*"}),
        policy_name="attached-policy",
    )
    client = _client("sqs", access_key)

    with enable_iam_authentication():
        client.list_queues()

    decision = get_auth_decisions()[0]
    policy_decision = decision.policy_decisions[0]
    assert policy_decision.policy_id == (
        f"arn:aws:iam::{ACCOUNT_ID}:policy/attached-policy"
    )
    assert policy_decision.result == "PERMITTED"


@mock_aws
def test_explicit_deny_is_recorded_with_matching_statement() -> None:
    admin = boto3.client("sqs", region_name="us-east-1")
    queue_url = admin.create_queue(QueueName="denied-queue")["QueueUrl"]

    access_key = create_user_with_access_key_and_inline_policy(
        "test-user",
        _policy({"Effect": "Deny", "Action": "sqs:SendMessage", "Resource": "*"}),
    )
    client = _client("sqs", access_key)

    with enable_iam_authentication(), pytest.raises(ClientError) as exc:
        client.send_message(QueueUrl=queue_url, MessageBody="hello")

    # The SQS query parser reports the HTTP status as the error code (this is
    # pre-existing behaviour); the AWS error code is visible on the record.
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403

    decisions = get_auth_decisions(denied_only=True)
    assert len(decisions) == 1
    decision = decisions[0]
    assert decision.action == "sqs:SendMessage"
    assert decision.effect == DecisionEffect.DENY
    assert decision.source == DecisionSource.POLICY
    assert decision.injected is False
    assert decision.deny_category == DenyCategory.EXPLICIT_DENY
    assert decision.error_code == "AccessDenied"

    statement = decision.policy_decisions[0].statements[0]
    assert statement.effect == "Deny"
    assert statement.action_matched is True
    assert statement.resource_matched is True
    assert statement.result == "DENIED"


@mock_aws
def test_implicit_deny_is_recorded() -> None:
    admin = boto3.client("sqs", region_name="us-east-1")
    queue_url = admin.create_queue(QueueName="implicit-queue")["QueueUrl"]

    # Only ListQueues is allowed; SendMessage matches no statement
    access_key = create_user_with_access_key_and_inline_policy(
        "test-user",
        _policy({"Effect": "Allow", "Action": "sqs:ListQueues", "Resource": "*"}),
    )
    client = _client("sqs", access_key)

    with enable_iam_authentication(), pytest.raises(ClientError):
        client.send_message(QueueUrl=queue_url, MessageBody="hello")

    decision = get_auth_decisions(denied_only=True)[0]
    assert decision.deny_category == DenyCategory.IMPLICIT_DENY
    assert all(
        statement.result == "NEUTRAL"
        for policy in decision.policy_decisions
        for statement in policy.statements
    )


@mock_aws
def test_signature_mismatch_is_recorded_as_signature_denial() -> None:
    access_key = create_user_with_access_key()
    client = _client("iam", access_key, secret_access_key="wrong-secret")

    with enable_iam_authentication(), pytest.raises(ClientError) as exc:
        client.get_user()

    assert exc.value.response["Error"]["Code"] == "SignatureDoesNotMatch"
    decision = get_auth_decisions(denied_only=True)[0]
    assert decision.effect == DecisionEffect.DENY
    assert decision.source == DecisionSource.SIGNATURE
    assert decision.deny_category == DenyCategory.SIGNATURE_MISMATCH
    assert decision.error_code == "SignatureDoesNotMatch"
    assert decision.injected is False
    assert decision.action == "iam:GetUser"
    assert decision.policy_decisions == ()


@mock_aws
def test_invalid_access_key_is_recorded() -> None:
    client = boto3.client(
        "iam",
        region_name="us-east-1",
        aws_access_key_id="AKIAINVALID0000000000",
        aws_secret_access_key="whatever",
    )

    with enable_iam_authentication(), pytest.raises(ClientError) as exc:
        client.get_user()

    assert exc.value.response["Error"]["Code"] == "InvalidClientTokenId"
    decision = get_auth_decisions(denied_only=True)[0]
    assert decision.deny_category == DenyCategory.INVALID_ACCESS_KEY
    assert decision.source == DecisionSource.SIGNATURE
    assert decision.service == "iam"
    assert decision.action == "GetUser"
    assert decision.principal is None


# ---------------------------------------------------------------------------
# Failure injection
# ---------------------------------------------------------------------------
@mock_aws
def test_injection_by_action_looks_like_real_denial_but_is_marked() -> None:
    access_key = create_user_with_access_key_and_inline_policy(
        "test-user", _policy({"Effect": "Allow", "Action": "sqs:*", "Resource": "*"})
    )
    client = _client("sqs", access_key)

    with enable_iam_authentication():
        queue_url = client.create_queue(QueueName="inject-action")["QueueUrl"]

        with inject_auth_failure(actions=["sqs:SendMessage"]) as rule_name:
            with pytest.raises(ClientError) as exc:
                client.send_message(QueueUrl=queue_url, MessageBody="hello")
            # Same denial path/status/message as a real explicit deny
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
            assert (
                "is not authorized to perform: sqs:SendMessage"
                in exc.value.response["Error"]["Message"]
            )

            # Non-matching actions still get the real (allow) decision
            client.list_queues()

        # Rule revoked: real decisions apply immediately
        client.send_message(QueueUrl=queue_url, MessageBody="hello")

    assert rule_name

    injected = get_auth_decisions(action="sqs:SendMessage", injected_only=True)
    assert len(injected) == 1
    decision = injected[0]
    assert decision.effect == DecisionEffect.DENY
    assert decision.source == DecisionSource.INJECTION
    assert decision.injected is True
    assert decision.deny_category == DenyCategory.INJECTED
    assert decision.error_code == "AccessDenied"
    assert decision.injection_rule is not None
    assert decision.injection_rule.name == rule_name
    assert decision.injection_rule.actions == ("sqs:SendMessage",)
    # Injection short-circuits policy evaluation - this is not a policy verdict
    assert decision.policy_decisions == ()

    real_send = get_auth_decisions(action="sqs:SendMessage", denied_only=False)[-1]
    assert real_send.injected is False
    assert real_send.effect == DecisionEffect.ALLOW


@mock_aws
def test_injection_bare_action_name_and_wildcard_selectors() -> None:
    access_key = create_user_with_access_key_and_inline_policy(
        "test-user", _policy({"Effect": "Allow", "Action": "sqs:*", "Resource": "*"})
    )
    client = _client("sqs", access_key)

    with enable_iam_authentication():
        queue_url = client.create_queue(QueueName="bare-selector")["QueueUrl"]
        with inject_auth_failure(actions=["List*"]):
            with pytest.raises(ClientError):
                client.list_queues()
            # SendMessage does not match "List*"
            client.send_message(QueueUrl=queue_url, MessageBody="hello")

    wildcard_denial = get_auth_decisions(action="sqs:List*", denied_only=True)
    assert len(wildcard_denial) == 1
    assert wildcard_denial[0].injected is True


@mock_aws
def test_injection_is_scoped_by_resource() -> None:
    access_key = create_user_with_access_key_and_inline_policy(
        "test-user", _policy({"Effect": "Allow", "Action": "sqs:*", "Resource": "*"})
    )
    client = _client("sqs", access_key)

    with enable_iam_authentication():
        queue_a = client.create_queue(QueueName="queue-a")["QueueUrl"]
        queue_b = client.create_queue(QueueName="queue-b")["QueueUrl"]
        arn_a = SQS_QUEUE_ARN.format(name="queue-a")
        arn_b = SQS_QUEUE_ARN.format(name="queue-b")

        with inject_auth_failure(actions=["sqs:SendMessage"], resources=[f"{arn_a}*"]):
            with pytest.raises(ClientError):
                client.send_message(QueueUrl=queue_a, MessageBody="to-a")
            client.send_message(QueueUrl=queue_b, MessageBody="to-b")

        # Action and resource selectors intersect: ReceiveMessage rule does
        # not affect SendMessage on the same resource
        with inject_auth_failure(actions=["sqs:ReceiveMessage"], resources=[arn_a]):
            client.send_message(QueueUrl=queue_a, MessageBody="to-a-again")

    denied = get_auth_decisions(denied_only=True)
    assert len(denied) == 1
    assert denied[0].resource == arn_a
    assert denied[0].action == "sqs:SendMessage"

    # The send to queue B was allowed with the real resource recorded
    send_b = next(
        d for d in get_auth_decisions(action="sqs:SendMessage") if d.resource == arn_b
    )
    assert send_b.effect == DecisionEffect.ALLOW
    assert send_b.injected is False


@mock_aws
def test_injection_removed_imperatively_restores_real_decisions() -> None:
    access_key = create_user_with_access_key_and_inline_policy(
        "test-user", _policy({"Effect": "Allow", "Action": "sqs:*", "Resource": "*"})
    )
    client = _client("sqs", access_key)

    with enable_iam_authentication():
        queue_url = client.create_queue(QueueName="imperative")["QueueUrl"]
        rule = add_auth_failure_injection(actions=["sqs:SendMessage"], name="my-rule")
        assert rule == "my-rule"
        with pytest.raises(ClientError):
            client.send_message(QueueUrl=queue_url, MessageBody="x")

        remove_auth_failure_injection("my-rule")
        client.send_message(QueueUrl=queue_url, MessageBody="y")

        # Removing an unknown rule is a no-op
        remove_auth_failure_injection("does-not-exist")

        with pytest.raises(ValueError):
            add_auth_failure_injection(name="my-rule")
        with pytest.raises(ValueError):
            add_auth_failure_injection()
        clear_auth_failure_injections()
        client.send_message(QueueUrl=queue_url, MessageBody="z")

    assert len(get_auth_decisions(action="sqs:SendMessage", denied_only=True)) == 1
    assert (
        get_auth_decisions(action="sqs:SendMessage")[-1].effect == DecisionEffect.ALLOW
    )


@mock_aws
def test_injection_can_target_get_caller_identity() -> None:
    # GetCallerIdentity is always allowed by the real implementation
    access_key = create_user_with_access_key_and_inline_policy(
        "test-user",
        _policy({"Effect": "Deny", "Action": "sts:GetCallerIdentity", "Resource": "*"}),
    )
    client = _client("sts", access_key)

    with enable_iam_authentication():
        identity = client.get_caller_identity()
        assert identity["Arn"] == USER_ARN

        with inject_auth_failure(actions=["sts:GetCallerIdentity"]), pytest.raises(
            ClientError
        ) as exc:
            client.get_caller_identity()
        assert exc.value.response["Error"]["Code"] == "AccessDenied"

    real = next(
        d for d in get_auth_decisions(action="sts:GetCallerIdentity") if not d.injected
    )
    assert real.effect == DecisionEffect.ALLOW
    assert get_auth_decisions(action="sts:GetCallerIdentity", injected_only=True)


# ---------------------------------------------------------------------------
# S3 keeps its own error semantics for injected denials
# ---------------------------------------------------------------------------
@mock_aws
def test_s3_injected_denial_reuses_s3_access_denied() -> None:
    access_key = create_user_with_access_key_and_inline_policy(
        "test-user", _policy({"Effect": "Allow", "Action": "s3:*", "Resource": "*"})
    )
    client = _client("s3", access_key)
    bucket = "injected-bucket"

    with enable_iam_authentication():
        client.create_bucket(Bucket=bucket)
        with inject_auth_failure(
            actions=["s3:PutObject"], resources=[f"arn:aws:s3:::{bucket}/*"]
        ):
            with pytest.raises(ClientError) as exc:
                client.put_object(Bucket=bucket, Key="blocked", Body=b"x")
            assert exc.value.response["Error"]["Code"] == "AccessDenied"
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403

        client.put_object(Bucket=bucket, Key="allowed", Body=b"y")

    blocked, allowed = get_auth_decisions(action="s3:PutObject")
    assert blocked.injected is True
    assert blocked.resource == f"arn:aws:s3:::{bucket}/blocked"
    assert allowed.injected is False
    assert allowed.effect == DecisionEffect.ALLOW
    assert allowed.resource == f"arn:aws:s3:::{bucket}/allowed"


# ---------------------------------------------------------------------------
# Querying: time windows, request ids, filters
# ---------------------------------------------------------------------------
@mock_aws
def test_decisions_are_filterable_by_time_request_and_pattern() -> None:
    access_key = create_user_with_access_key_and_inline_policy(
        "test-user", _policy({"Effect": "Allow", "Action": "sqs:*", "Resource": "*"})
    )
    client = _client("sqs", access_key)

    with enable_iam_authentication():
        client.list_queues()
        time.sleep(0.02)
        client.create_queue(QueueName="filtered")
        second = get_auth_decisions()[-1]

    assert len(get_auth_decisions(since=second.timestamp)) == 1
    assert len(get_auth_decisions(until=second.timestamp)) == 1
    assert (
        len(get_auth_decisions(request_id=second.request_id, since=second.timestamp))
        == 1
    )
    assert get_auth_decisions(request_id="unknown") == ()
    assert len(get_auth_decisions(action="sqs:Create*")) == 1
    assert len(get_auth_decisions(denied_only=True)) == 0


@mock_aws
def test_every_decision_has_a_unique_request_id_and_monotonic_sequence() -> None:
    access_key = create_user_with_access_key_and_inline_policy(
        "test-user", _policy({"Effect": "Allow", "Action": "sqs:*", "Resource": "*"})
    )
    client = _client("sqs", access_key)

    with enable_iam_authentication():
        for _ in range(5):
            client.list_queues()

    decisions = get_auth_decisions()
    assert len({d.request_id for d in decisions}) == 5
    assert [d.sequence for d in decisions] == [1, 2, 3, 4, 5]


# ---------------------------------------------------------------------------
# Capacity and dropped-record counter
# ---------------------------------------------------------------------------
@mock_aws
def test_old_records_are_dropped_at_capacity_and_count_is_exposed() -> None:
    access_key = create_user_with_access_key_and_inline_policy(
        "test-user", _policy({"Effect": "Allow", "Action": "sqs:*", "Resource": "*"})
    )
    client = _client("sqs", access_key)

    configure_auth_decisions(max_records=3)
    with enable_iam_authentication():
        for _ in range(5):
            client.list_queues()

    decisions = get_auth_decisions()
    assert len(decisions) == 3
    assert [d.sequence for d in decisions] == [3, 4, 5]
    assert dropped_auth_decisions() == 2

    # Shrinking the capacity drops the oldest immediately
    configure_auth_decisions(max_records=1)
    assert dropped_auth_decisions() == 4
    assert [d.sequence for d in get_auth_decisions()] == [5]

    reset_auth_decisions()
    assert dropped_auth_decisions() == 0
    assert get_auth_decisions() == ()


@mock_aws
def test_zero_capacity_drops_everything() -> None:
    configure_auth_decisions(max_records=0)
    access_key = create_user_with_access_key_and_inline_policy(
        "test-user", _policy({"Effect": "Allow", "Action": "sqs:*", "Resource": "*"})
    )
    client = _client("sqs", access_key)

    with enable_iam_authentication():
        client.list_queues()

    assert get_auth_decisions() == ()
    assert dropped_auth_decisions() == 1


def test_capacity_must_be_non_negative_integer() -> None:
    with pytest.raises(ValueError):
        configure_auth_decisions(max_records=-1)


# ---------------------------------------------------------------------------
# HTTP surface (used in server mode)
# ---------------------------------------------------------------------------
@mock_aws
def test_moto_api_endpoints_expose_decisions_and_injections() -> None:
    import requests as http_requests

    base_url = "http://motoapi.amazonaws.com"

    access_key = create_user_with_access_key_and_inline_policy(
        "test-user", _policy({"Effect": "Allow", "Action": "sqs:*", "Resource": "*"})
    )
    client = _client("sqs", access_key)

    with enable_iam_authentication():
        client.list_queues()
        response = http_requests.get(f"{base_url}/moto-api/auth/decisions")
        body = response.json()
        assert response.status_code == 200
        assert len(body["decisions"]) == 1
        assert body["decisions"][0]["action"] == "sqs:ListQueues"
        assert body["dropped_records"] == 0
        assert body["max_records"] >= 1

        # Configure injection over HTTP - the next matching call is denied
        response = http_requests.post(
            f"{base_url}/moto-api/auth/injections",
            json={"name": "http-rule", "actions": "sqs:CreateQueue"},
        )
        assert response.json()["name"] == "http-rule"
        with pytest.raises(ClientError):
            client.create_queue(QueueName="http-injected")

        denied = http_requests.get(
            f"{base_url}/moto-api/auth/decisions",
            params={"denied_only": "true", "injected_only": "true"},
        ).json()["decisions"]
        assert len(denied) == 1
        assert denied[0]["deny_category"] == DenyCategory.INJECTED
        assert denied[0]["injection_rule"]["name"] == "http-rule"
        assert denied[0]["injection_rule"]["actions"] == ["sqs:CreateQueue"]

        # Revoke over HTTP - real decisions apply immediately
        http_requests.post(
            f"{base_url}/moto-api/auth/injections/remove", json={"name": "http-rule"}
        )
        client.create_queue(QueueName="http-allowed")

        # Capacity can be configured over HTTP
        http_requests.post(
            f"{base_url}/moto-api/auth/decisions/configure",
            json={"max_records": 1},
        )
        client.list_queues()
        summary = http_requests.get(f"{base_url}/moto-api/auth/decisions").json()
        assert len(summary["decisions"]) == 1
        assert summary["dropped_records"] >= 1

        # Reset over HTTP empties decisions and rules
        http_requests.post(f"{base_url}/moto-api/auth/decisions/reset")
        summary = http_requests.get(f"{base_url}/moto-api/auth/decisions").json()
        assert summary["decisions"] == []
        assert summary["dropped_records"] == 0


@mock_aws
def test_full_moto_reset_clears_decisions_and_injections() -> None:
    import requests as http_requests

    access_key = create_user_with_access_key_and_inline_policy(
        "test-user", _policy({"Effect": "Allow", "Action": "sqs:*", "Resource": "*"})
    )
    client = _client("sqs", access_key)

    with enable_iam_authentication():
        client.list_queues()
        add_auth_failure_injection(actions=["sqs:SendMessage"], name="leak-rule")

    assert len(get_auth_decisions()) == 1

    http_requests.post("http://motoapi.amazonaws.com/moto-api/reset")

    assert get_auth_decisions() == ()
    assert get_auth_decision_log().injection_rules() == ()


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------
def test_recording_is_thread_safe_and_requests_cannot_mix() -> None:
    log = get_auth_decision_log()
    log.configure(max_records=10_000)

    def worker(worker_id: int) -> None:
        for _ in range(100):
            evaluation = AuthorizationEvaluation(
                action=f"svc{worker_id}:Action",
                resource=f"arn:svc{worker_id}:resource",
                principal=f"principal-{worker_id}",
            )
            evaluation.allow()
            log.record(
                request_id=uuid4().hex,
                account_id=str(worker_id),
                region="us-east-1",
                service=f"svc{worker_id}",
                evaluation=evaluation,
            )

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    records = log.decisions()
    assert len(records) == 800
    assert len({r.request_id for r in records}) == 800
    assert {r.sequence for r in records} == set(range(1, 801))
    # No record picked up another request's action/principal
    for record in records:
        worker_id = record.account_id
        assert record.action == f"svc{worker_id}:Action"
        assert record.principal == f"principal-{worker_id}"
        assert record.resource == f"arn:svc{worker_id}:resource"


@mock_aws
def test_concurrent_authorized_requests_all_get_their_own_records() -> None:
    access_key = create_user_with_access_key_and_inline_policy(
        "test-user", _policy({"Effect": "Allow", "Action": "sqs:*", "Resource": "*"})
    )

    with enable_iam_authentication():
        client = _client("sqs", access_key)
        queue_urls = [
            client.create_queue(QueueName=f"parallel-{i}")["QueueUrl"] for i in range(6)
        ]

        errors: list[Exception] = []

        def send(queue_url: str) -> None:
            try:
                client.send_message(QueueUrl=queue_url, MessageBody="parallel")
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=send, args=(url,)) for url in queue_urls]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == []
        send_decisions = get_auth_decisions(action="sqs:SendMessage")
        assert len(send_decisions) == 6
        assert len({d.request_id for d in send_decisions}) == 6
        assert all(d.effect == DecisionEffect.ALLOW for d in send_decisions)
        assert all(not d.injected for d in send_decisions)

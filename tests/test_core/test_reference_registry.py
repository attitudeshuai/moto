from collections.abc import Iterator

import pytest

from moto.core.references.coordinates import ResourceCoordinate
from moto.core.references.policy import PolicyValue, policy_book
from moto.core.references.records import ReferenceRecord
from moto.core.references.registry import reference_registry


def _queue(
    account_id: str = "123456789012",
    region: str = "us-east-1",
    name: str = "test-queue",
) -> ResourceCoordinate:
    return ResourceCoordinate(
        service="sqs",
        account_id=account_id,
        region=region,
        resource_type="queue",
        resource_id=name,
    )


@pytest.fixture(autouse=True)
def _reset_registry() -> Iterator[None]:
    reference_registry.reset()
    policy_book.reset()
    yield
    reference_registry.reset()
    policy_book.reset()


def test_register_and_list_referrers() -> None:
    target = _queue()
    subscription = ResourceCoordinate(
        service="sns",
        account_id="123456789012",
        region="us-east-1",
        resource_type="subscription",
        resource_id="sub-1",
    )
    mapping = ResourceCoordinate(
        service="lambda",
        account_id="210987654321",
        region="us-west-2",
        resource_type="event_source_mapping",
        resource_id="esm-1",
    )

    reference_registry.register(subscription, target, "Subscription")
    reference_registry.register(mapping, target, "EventSourceMapping")

    referrers = reference_registry.list_referrers(target)
    assert len(referrers) == 2
    by_relation = {record.relation: record for record in referrers}
    sub_record = by_relation["Subscription"]
    assert sub_record.source == subscription
    assert sub_record.target == target
    assert sub_record.source.service == "sns"
    assert sub_record.source.account_id == "123456789012"
    assert sub_record.source.region == "us-east-1"
    esm_record = by_relation["EventSourceMapping"]
    assert esm_record.source.service == "lambda"
    assert esm_record.source.account_id == "210987654321"
    assert esm_record.source.region == "us-west-2"

    # Filters
    assert reference_registry.list_referrers(target, source_service="sns") == [
        sub_record
    ]
    assert reference_registry.list_referrers(
        target, source_account_id="210987654321"
    ) == [esm_record]
    assert reference_registry.list_referrers(target, source_region="us-west-2") == [
        esm_record
    ]
    assert reference_registry.list_referrers(target, relation="EventSourceMapping") == [
        esm_record
    ]
    assert reference_registry.list_referrers(target, source_service="ec2") == []


def test_register_is_idempotent() -> None:
    source = ResourceCoordinate("sns", "123456789012", "us-east-1", "subscription", "s")
    target = _queue()

    first = reference_registry.register(source, target, "Subscription")
    second = reference_registry.register(source, target, "Subscription")

    assert first is second
    assert len(reference_registry.list_referrers(target)) == 1
    assert second.registered_at == first.registered_at

    # Metadata update preserves the timestamp.
    updated = reference_registry.register(
        source, target, "Subscription", metadata={"RawMessageDelivery": True}
    )
    assert len(reference_registry.list_referrers(target)) == 1
    assert dict(updated.metadata) == {"RawMessageDelivery": True}
    assert updated.registered_at == first.registered_at


def test_unregister() -> None:
    source = ResourceCoordinate("sns", "123456789012", "us-east-1", "subscription", "s")
    target = _queue()
    reference_registry.register(source, target, "Subscription")

    assert reference_registry.unregister(source, target, "Subscription") is True
    assert reference_registry.list_referrers(target) == []
    assert reference_registry.unregister(source, target, "Subscription") is False


def test_unregister_source() -> None:
    source = ResourceCoordinate("sns", "123456789012", "us-east-1", "subscription", "s")
    queue_a = _queue(name="a")
    queue_b = _queue(name="b")
    reference_registry.register(source, queue_a, "Subscription")
    reference_registry.register(source, queue_b, "Subscription")

    removed = reference_registry.unregister_source(source)

    assert removed == 2
    assert reference_registry.list_referrers(queue_a) == []
    assert reference_registry.list_referrers(queue_b) == []


def test_replace_target_atomic() -> None:
    source = ResourceCoordinate(
        "lambda", "123456789012", "us-east-1", "event_source_mapping", "m"
    )
    old_queue = _queue(name="old")
    new_queue = _queue(name="new")
    original = reference_registry.register(source, old_queue, "EventSourceMapping")

    new_record = reference_registry.replace_target(
        source, old_queue, new_queue, "EventSourceMapping"
    )

    assert isinstance(new_record, ReferenceRecord)
    assert reference_registry.list_referrers(old_queue) == []
    assert [
        record.source for record in reference_registry.list_referrers(new_queue)
    ] == [source]
    # Timestamp is preserved across the move.
    assert new_record.registered_at == original.registered_at

    with pytest.raises(KeyError):
        reference_registry.replace_target(
            source, old_queue, new_queue, "EventSourceMapping"
        )

    # Replacing onto an already existing edge is rejected.
    reference_registry.register(source, old_queue, "OtherRelation")
    reference_registry.register(source, new_queue, "OtherRelation")
    with pytest.raises(ValueError):
        reference_registry.replace_target(source, old_queue, new_queue, "OtherRelation")


def test_coordinate_from_arn() -> None:
    # resource id only (SQS-style)
    queue_arn = "arn:aws:sqs:us-east-1:123456789012:my-queue"
    coordinate = ResourceCoordinate.from_arn(queue_arn)
    assert coordinate == ResourceCoordinate(
        service="sqs",
        account_id="123456789012",
        region="us-east-1",
        resource_type=None,
        resource_id="my-queue",
    )

    # slash-separated resource type
    sub_arn = "arn:aws:sns:us-east-1:123456789012:subscription/abcd"
    sub_coordinate = ResourceCoordinate.from_arn(sub_arn)
    assert sub_coordinate.service == "sns"
    assert sub_coordinate.resource_type == "subscription"
    assert sub_coordinate.resource_id == "abcd"

    # colon-separated resource type
    fn_arn = "arn:aws:lambda:us-west-2:123456789012:function:my-fn"
    fn_coordinate = ResourceCoordinate.from_arn(fn_arn)
    assert fn_coordinate.service == "lambda"
    assert fn_coordinate.region == "us-west-2"
    assert fn_coordinate.resource_type == "function"
    assert fn_coordinate.resource_id == "my-fn"


def test_partial_target_query_across_accounts_and_regions() -> None:
    source_a = ResourceCoordinate(
        "sns", "111111111111", "us-east-1", "subscription", "s1"
    )
    source_b = ResourceCoordinate(
        "sns", "222222222222", "eu-west-1", "subscription", "s2"
    )
    queue_a = _queue(account_id="111111111111", region="us-east-1", name="shared-name")
    queue_b = _queue(account_id="222222222222", region="eu-west-1", name="shared-name")
    reference_registry.register(source_a, queue_a, "Subscription")
    reference_registry.register(source_b, queue_b, "Subscription")

    global_pattern = ResourceCoordinate(
        service="sqs",
        account_id=None,
        region=None,
        resource_type="queue",
        resource_id="shared-name",
    )
    referrers = reference_registry.list_referrers(global_pattern)
    assert {record.source.account_id for record in referrers} == {
        "111111111111",
        "222222222222",
    }

    account_pattern = ResourceCoordinate(
        service="sqs",
        account_id="111111111111",
        region=None,
        resource_type="queue",
        resource_id="shared-name",
    )
    account_referrers = reference_registry.list_referrers(account_pattern)
    assert [record.source.account_id for record in account_referrers] == [
        "111111111111"
    ]


# ---------------------------------------------------------------------------
# Policy resolution
# ---------------------------------------------------------------------------


def test_policy_defaults_to_passive() -> None:
    resolution = policy_book.resolve(_queue())
    assert resolution.policy is None
    assert resolution.to_dict()["policy"] == "passive"
    assert resolution.source == "default"


def test_policy_priority_matrix() -> None:
    target = _queue(account_id="123456789012", region="us-east-1", name="q")

    # service-only match
    policy_book.set_service_policy("sqs", PolicyValue.WARN)
    resolution = policy_book.resolve(target)
    assert resolution.policy is PolicyValue.WARN
    assert resolution.source == "service"

    # account-wide beats service
    policy_book.set_account_policy("123456789012", PolicyValue.DENY)
    resolution = policy_book.resolve(target)
    assert resolution.policy is PolicyValue.DENY
    assert resolution.source == "account"

    # account+service beats account-wide
    policy_book.set_account_policy("123456789012", PolicyValue.CASCADE, service="sqs")
    resolution = policy_book.resolve(target)
    assert resolution.policy is PolicyValue.CASCADE
    assert resolution.source == "account+service"

    # An unrelated account falls back to service policy.
    other = policy_book.resolve(_queue(account_id="999999999999", name="q"))
    assert other.policy is PolicyValue.WARN
    assert other.source == "service"


def test_service_resource_type_refinement() -> None:
    target = ResourceCoordinate(
        service="sqs",
        account_id="123456789012",
        region="us-east-1",
        resource_type="queue",
        resource_id="q",
    )
    policy_book.set_service_policy("sqs", PolicyValue.WARN)
    policy_book.set_service_policy("sqs", PolicyValue.DENY, resource_type="queue")

    resolution = policy_book.resolve(target)
    assert resolution.policy is PolicyValue.DENY
    assert resolution.source == "service+resource_type"

    # account dimension still outranks service+resource_type
    policy_book.set_account_policy("123456789012", PolicyValue.CASCADE)
    resolution = policy_book.resolve(target)
    assert resolution.policy is PolicyValue.CASCADE
    assert resolution.source == "account"


def test_global_policy_fallback() -> None:
    target = _queue()
    policy_book.set_global_policy(PolicyValue.WARN)

    resolution = policy_book.resolve(target)
    assert resolution.policy is PolicyValue.WARN
    assert resolution.source == "global"


def test_policy_set_get_delete() -> None:
    policy_book.set_service_policy("sqs", PolicyValue.DENY)
    assert policy_book.get_service_policy("sqs") is PolicyValue.DENY
    assert any(entry["policy"] == "deny" for entry in policy_book.list_policies())

    assert policy_book.delete_service_policy("sqs") is True
    assert policy_book.get_service_policy("sqs") is None
    assert policy_book.delete_service_policy("sqs") is False

    policy_book.set_account_policy("123456789012", PolicyValue.WARN)
    assert policy_book.get_account_policy("123456789012") is PolicyValue.WARN
    assert policy_book.delete_account_policy("123456789012") is True

    policy_book.set_global_policy(PolicyValue.CASCADE)
    assert policy_book.get_global_policy() is PolicyValue.CASCADE
    assert policy_book.delete_global_policy() is True


def test_backenddict_reset_clears_reference_subsystem() -> None:
    from moto.core.base_backend import BackendDict
    from moto.core.references import (
        PolicyValue,
        list_referrers,
        list_warnings,
        register_reference,
        resolve_reference_policy,
        run_audit,
    )

    source = ResourceCoordinate(
        "sns", "123456789012", "us-east-1", "subscription", "sub-1"
    )
    target = _queue()
    register_reference(source, target, "Subscription")
    policy_book.set_service_policy("sqs", PolicyValue.WARN)
    # Emit a warning through the real protocol.
    from moto.core.references import guarded_operation

    guarded_operation(target, lambda: None)
    assert list_warnings(target) != []

    BackendDict.reset()

    assert list_referrers(target) == []
    assert list_warnings(target) == []
    assert resolve_reference_policy(target).policy is None
    assert run_audit() == []
    assert reference_registry.is_tombstoned(target) is False


def test_policy_reset_clears_all() -> None:
    policy_book.set_global_policy(PolicyValue.DENY)
    policy_book.set_service_policy("sqs", PolicyValue.WARN)
    policy_book.set_account_policy("123456789012", PolicyValue.CASCADE)

    policy_book.reset()

    assert policy_book.get_global_policy() is None
    assert policy_book.get_service_policy("sqs") is None
    assert policy_book.get_account_policy("123456789012") is None
    assert policy_book.list_policies() == []

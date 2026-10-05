from collections.abc import Callable, Iterator

import pytest

from moto.core.references.adapters import adapter_registry
from moto.core.references.coordinates import ResourceCoordinate
from moto.core.references.exceptions import ReferenceViolation
from moto.core.references.policy import PolicyValue, policy_book
from moto.core.references.protocol import guarded_operation
from moto.core.references.registry import reference_registry
from moto.core.references.warnings import warning_log

_STUB_SERVICES = ("stubsns", "stublambda", "stubext", "unknownsvc")


def _coord(
    service: str,
    resource_id: str,
    resource_type: str = "queue",
    account_id: str = "123456789012",
    region: str = "us-east-1",
) -> ResourceCoordinate:
    return ResourceCoordinate(
        service=service,
        account_id=account_id,
        region=region,
        resource_type=resource_type,
        resource_id=resource_id,
    )


@pytest.fixture(autouse=True)
def _reset() -> Iterator[None]:
    reference_registry.reset()
    policy_book.reset()
    warning_log.reset()
    yield
    reference_registry.reset()
    policy_book.reset()
    warning_log.reset()


@pytest.fixture
def stub_deleters() -> dict[str, list[str]]:
    calls: dict[str, list[str]] = {"deleted": []}

    def _make(resource_id: str) -> Callable[[ResourceCoordinate], None]:
        def _deleter(coordinate: ResourceCoordinate) -> None:
            calls["deleted"].append(resource_id)

        return _deleter

    adapter_registry.register_deleter("stubsns", "subscription", _make("subscription"))
    adapter_registry.register_deleter(
        "stublambda", "event_source_mapping", _make("mapping")
    )
    adapter_registry.register_deleter("stubext", "rule_target", _make("rule_target"))
    return calls


def _target() -> ResourceCoordinate:
    return _coord("sqs", "my-queue", "queue")


# TR-3.1 deny ----------------------------------------------------------------


def test_deny_policy_aborts_with_references(
    stub_deleters: dict[str, list[str]],
) -> None:
    target = _target()
    source = _coord("stubsns", "sub-1", "subscription")
    reference_registry.register(source, target, "Subscription")
    policy_book.set_service_policy("sqs", PolicyValue.DENY)

    action_calls = []

    def _action() -> None:
        action_calls.append(1)

    with pytest.raises(ReferenceViolation) as exc_info:
        guarded_operation(target, _action)

    error = exc_info.value
    assert error.reason == "policy_deny"
    assert len(error.references) == 1
    assert error.references[0].source == source
    assert action_calls == []
    assert stub_deleters["deleted"] == []
    assert reference_registry.is_tombstoned(target) is False
    # Edge is retained; target untouched and still referenced.
    assert [r.source for r in reference_registry.list_referrers(target)] == [source]


# TR-3.2 warn ----------------------------------------------------------------


def test_warn_policy_records_warning_and_proceeds() -> None:
    target = _target()
    source = _coord("stubsns", "sub-1", "subscription")
    reference_registry.register(source, target, "Subscription")
    policy_book.set_service_policy("sqs", PolicyValue.WARN)

    action_calls = []

    def _action() -> str:
        action_calls.append(1)
        return "done"

    result = guarded_operation(target, _action)

    assert result == "done"
    assert action_calls == [1]
    warnings = warning_log.list_warnings(target)
    assert len(warnings) == 1
    assert warnings[0].operation == "delete"
    assert [r.source for r in warnings[0].references] == [source]
    assert reference_registry.is_tombstoned(target) is False
    # Edge retained for later audit detection (missing target).
    assert len(reference_registry.list_referrers(target)) == 1


# TR-3.3 cascade -------------------------------------------------------------


def test_cascade_deletes_referrers_layer_by_layer(
    stub_deleters: dict[str, list[str]],
) -> None:
    target = _target()
    layer_one = _coord("stubsns", "sub-1", "subscription")
    layer_two = _coord("stublambda", "esm-1", "event_source_mapping")
    reference_registry.register(layer_one, target, "Subscription")
    reference_registry.register(layer_two, layer_one, "EventSourceMapping")
    policy_book.set_service_policy("sqs", PolicyValue.CASCADE)

    action_calls = []

    def _action() -> None:
        action_calls.append(1)

    guarded_operation(target, _action)

    # deepest layer deleted first, then layer one, then the target action runs
    assert stub_deleters["deleted"] == ["mapping", "subscription"]
    assert action_calls == [1]
    assert reference_registry.list_referrers(target) == []
    assert reference_registry.is_tombstoned(target) is False
    # Edges of deleted referrers are cleaned as well.
    assert reference_registry.list_referrers(layer_one) == []


def test_cascade_single_level(
    stub_deleters: dict[str, list[str]],
) -> None:
    target = _target()
    source = _coord("stubext", "rt-1", "rule_target")
    reference_registry.register(source, target, "RuleTarget")
    policy_book.set_service_policy("sqs", PolicyValue.CASCADE)

    guarded_operation(target, lambda: None)

    assert stub_deleters["deleted"] == ["rule_target"]
    assert reference_registry.list_referrers(target) == []


# TR-3.4 adapter gap / cycle / depth ----------------------------------------


def test_cascade_missing_deleter_aborts_and_keeps_everything(
    stub_deleters: dict[str, list[str]],
) -> None:
    target = _target()
    unknown = _coord("unknownsvc", "thing-1", "thing")
    reference_registry.register(unknown, target, "SomeRelation")
    policy_book.set_service_policy("sqs", PolicyValue.CASCADE)

    action_calls = []

    def _action() -> None:
        action_calls.append(1)

    with pytest.raises(ReferenceViolation) as exc_info:
        guarded_operation(target, _action)

    assert exc_info.value.reason == "cascade_impossible"
    assert action_calls == []
    assert stub_deleters["deleted"] == []
    assert reference_registry.is_tombstoned(target) is False
    # Nothing was torn down: edge remains, target presumed present.
    assert [r.source for r in reference_registry.list_referrers(target)] == [unknown]


def test_cascade_cycle_is_rejected(
    stub_deleters: dict[str, list[str]],
) -> None:
    target = _target()
    sub = _coord("stubsns", "sub-1", "subscription")
    mapping = _coord("stublambda", "esm-1", "event_source_mapping")
    reference_registry.register(sub, target, "Subscription")
    reference_registry.register(mapping, sub, "EventSourceMapping")
    reference_registry.register(sub, mapping, "BackReference")
    policy_book.set_service_policy("sqs", PolicyValue.CASCADE)

    action_calls = []

    def _action() -> None:
        action_calls.append(1)

    with pytest.raises(ReferenceViolation) as exc_info:
        guarded_operation(target, _action)

    assert exc_info.value.reason == "cascade_cycle"
    assert action_calls == []
    assert stub_deleters["deleted"] == []
    assert reference_registry.is_tombstoned(target) is False
    # All edges remain intact.
    assert len(reference_registry.list_referrers(target)) == 1
    assert len(reference_registry.list_referrers(sub)) == 1
    assert len(reference_registry.list_referrers(mapping)) == 1


def test_cascade_depth_limit(
    stub_deleters: dict[str, list[str]],
) -> None:
    target = _target()
    sub = _coord("stubsns", "sub-1", "subscription")
    mapping = _coord("stublambda", "esm-1", "event_source_mapping")
    reference_registry.register(sub, target, "Subscription")
    reference_registry.register(mapping, sub, "EventSourceMapping")
    policy_book.set_service_policy("sqs", PolicyValue.CASCADE)

    with pytest.raises(ReferenceViolation) as exc_info:
        guarded_operation(target, lambda: None, cascade_depth=1)

    assert exc_info.value.reason == "cascade_depth"
    assert stub_deleters["deleted"] == []
    assert reference_registry.is_tombstoned(target) is False


# TR-3.5 action failure ------------------------------------------------------


def test_action_exception_clears_tombstone_and_keeps_edges() -> None:
    target = _target()
    source = _coord("stubsns", "sub-1", "subscription")
    reference_registry.register(source, target, "Subscription")

    def _failing_action() -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        guarded_operation(target, _failing_action)

    assert reference_registry.is_tombstoned(target) is False
    assert [r.source for r in reference_registry.list_referrers(target)] == [source]


# TR-3.6 tombstone blocks concurrent registration ---------------------------


def test_registration_onto_tombstoned_target_fails() -> None:
    target = _target()
    reference_registry.add_tombstones({target})

    source = _coord("stubsns", "sub-1", "subscription")
    with pytest.raises(ReferenceViolation) as exc_info:
        reference_registry.register(source, target, "Subscription")
    assert exc_info.value.reason == "target_deleting"

    reference_registry.remove_tombstones({target})
    record = reference_registry.register(source, target, "Subscription")
    assert record.source == source


def test_registration_blocked_during_passive_action() -> None:
    target = _target()
    source = _coord("stubsns", "sub-1", "subscription")

    def _action() -> None:
        # While the action runs the target is tombstoned.
        assert reference_registry.is_tombstoned(target) is True
        with pytest.raises(ReferenceViolation) as exc_info:
            reference_registry.register(source, target, "Subscription")
        assert exc_info.value.reason == "target_deleting"

    guarded_operation(target, _action)

    assert reference_registry.is_tombstoned(target) is False

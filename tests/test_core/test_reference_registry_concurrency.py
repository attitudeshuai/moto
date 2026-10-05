import threading
from collections.abc import Iterator
from queue import Queue
from typing import Any

import pytest

from moto.core.references import (
    PolicyValue,
    ResourceCoordinate,
    guarded_operation,
    list_warnings,
    reference_registry,
    run_audit,
    set_service_reference_policy,
)
from moto.core.references.adapters import adapter_registry
from moto.core.references.audit import AuditFilters
from moto.core.references.exceptions import ReferenceViolation

SERVICE = "stresssvc"
REGION = "us-east-1"
ACCOUNT = "123456789012"

TARGET_COUNT = 12
SOURCE_COUNT = 40

# Shared, test-wide view of which targets have actually been deleted.
_DELETED: set[ResourceCoordinate] = set()
_deleted_lock = threading.Lock()


def _targets() -> list[ResourceCoordinate]:
    return [
        ResourceCoordinate(
            service=SERVICE,
            account_id=ACCOUNT,
            region=REGION,
            resource_type="queue",
            resource_id=f"q{i}",
        )
        for i in range(TARGET_COUNT)
    ]


def _sources() -> list[ResourceCoordinate]:
    return [
        ResourceCoordinate(
            service=SERVICE,
            account_id=ACCOUNT,
            region=REGION,
            resource_type="thing",
            resource_id=f"s{i}",
        )
        for i in range(SOURCE_COUNT)
    ]


def _register_test_adapters() -> None:
    def _exists(coordinate: ResourceCoordinate) -> bool:
        with _deleted_lock:
            return coordinate not in _DELETED

    def _extract() -> Iterator[Any]:
        # The test's sources "actually hold" whatever edges remain registered.
        with reference_registry.lock:
            edges = list(reference_registry._edges)  # noqa: SLF001
        for source, target, relation in edges:
            if source.service == SERVICE and source.resource_type == "thing":
                yield (source, target, relation, {})

    adapter_registry.register_existence_checker(SERVICE, "queue", _exists)
    adapter_registry.register_extractor(SERVICE, "thing", _extract)


def _worker(
    kind: str,
    targets: list[ResourceCoordinate],
    sources: list[ResourceCoordinate],
    stop: threading.Event,
    errors: Queue[BaseException],
) -> None:
    try:
        while not stop.is_set():
            if kind == "register":
                source = sources[threading.get_ident() % len(sources)]
                target = targets[(threading.get_ident() // 7) % len(targets)]
                try:
                    reference_registry.register(source, target, "stress_ref")
                except ReferenceViolation:
                    # Target being concurrently deleted.
                    pass
            elif kind in ("unregister", "repoint"):
                with reference_registry.lock:
                    keys = list(reference_registry._edges)  # noqa: SLF001
                if keys:
                    source, target, relation = keys[threading.get_ident() % len(keys)]
                    if kind == "unregister":
                        reference_registry.unregister(source, target, relation)
                    else:
                        new_target = targets[
                            (threading.get_ident() // 3) % len(targets)
                        ]
                        try:
                            reference_registry.replace_target(
                                source,
                                target,
                                new_target,
                                relation,
                            )
                        except (
                            KeyError,
                            ValueError,
                            ReferenceViolation,
                        ):
                            # edge vanished/edge exists/target being deleted
                            pass
            else:
                target = targets[threading.get_ident() % len(targets)]

                def _action(t: ResourceCoordinate = target) -> None:
                    with _deleted_lock:
                        _DELETED.add(t)

                try:
                    guarded_operation(target, _action)
                except ReferenceViolation:
                    # deny with references, or concurrent guarded op.
                    pass
    except BaseException as exc:  # pragma: no cover - failure capture
        errors.put(exc)


@pytest.mark.parametrize("policy", ["passive", "warn", "deny"])
def test_concurrent_register_unregister_and_delete(policy: str) -> None:
    _register_test_adapters()
    reference_registry.reset()
    with _deleted_lock:
        _DELETED.clear()

    if policy != "passive":
        set_service_reference_policy(
            SERVICE,
            PolicyValue.WARN if policy == "warn" else PolicyValue.DENY,
        )

    targets = _targets()
    sources = _sources()
    stop = threading.Event()
    errors: Queue[BaseException] = Queue()

    thread_specs = (
        ["register"] * 4 + ["unregister"] * 2 + ["repoint"] * 2 + ["delete"] * 3
    )
    threads = [
        threading.Thread(
            target=_worker,
            args=(kind, targets, sources, stop, errors),
            daemon=True,
        )
        for kind in thread_specs
    ]

    for thread in threads:
        thread.start()

    timer = threading.Timer(2.0, stop.set)
    timer.start()
    for thread in threads:
        thread.join(timeout=10)
    timer.join()

    assert errors.empty(), f"worker raised: {errors.get_nowait()}"

    # -- post-conditions ----------------------------------------------------

    # 1. No tombstone can survive the protocol.
    assert reference_registry._tombstones == set()  # noqa: SLF001

    # 2. The edge store and both indexes agree.
    with reference_registry.lock:
        all_edges = set(reference_registry._edges)  # noqa: SLF001
        reverse_index = {
            coordinate: set(keys)
            for coordinate, keys in reference_registry._by_target.items()  # noqa: SLF001
        }
        forward_index = {
            coordinate: set(keys)
            for coordinate, keys in reference_registry._by_source.items()  # noqa: SLF001
        }

    for key in all_edges:
        source, target, _relation = key
        assert key in reverse_index[target]
        assert key in forward_index[source]
    for keys in reverse_index.values():
        assert keys <= all_edges
    for keys in forward_index.values():
        assert keys <= all_edges

    # 3. Every edge onto a deleted target is reported as missing_target; there
    #    are no untraced dangling edges.
    findings = run_audit(AuditFilters(type="missing_target"))
    dangling = {
        (finding.source, finding.target, finding.relation) for finding in findings
    }
    expected_dangling = {key for key in all_edges if key[1] in _DELETED}
    assert dangling == expected_dangling

    # 4. warn policy: every deleted target has at least one warning.
    if policy == "warn":
        warnings = list_warnings()
        warned_targets = {warning.target for warning in warnings}
        assert _DELETED <= warned_targets

    reference_registry.reset()
    with _deleted_lock:
        _DELETED.clear()

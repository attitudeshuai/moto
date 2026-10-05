from collections.abc import Iterable, Iterator
from typing import cast

import pytest

from moto.core.references.adapters import (
    ExtractedEdge,
    adapter_registry,
)
from moto.core.references.audit import AuditFilters, Inconsistency, run_audit
from moto.core.references.coordinates import ResourceCoordinate
from moto.core.references.policy import policy_book
from moto.core.references.registry import reference_registry
from moto.core.references.warnings import warning_log


def _coord(
    service: str,
    resource_id: str,
    resource_type: str,
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


@pytest.fixture
def stub_adapters() -> Iterator[dict[str, object]]:
    state: dict[str, object] = {
        "actual_edges": [],
        "target_exists": True,
    }

    def _extractor() -> Iterable[ExtractedEdge]:
        return cast(Iterable[ExtractedEdge], state["actual_edges"])

    def _checker(coordinate: ResourceCoordinate) -> bool:
        return bool(state["target_exists"])

    adapter_registry.register_extractor("auditsns", "subscription", _extractor)
    adapter_registry.register_existence_checker("auditsqs", "queue", _checker)
    yield state
    # Return the code-time stubs to an inert state so they cannot pollute
    # other tests running in the same process.
    state["actual_edges"] = []
    state["target_exists"] = True


@pytest.fixture(autouse=True)
def _reset() -> Iterator[None]:
    reference_registry.reset()
    policy_book.reset()
    warning_log.reset()
    yield
    reference_registry.reset()
    policy_book.reset()
    warning_log.reset()


def _edges() -> tuple[
    tuple[ResourceCoordinate, ResourceCoordinate],
    tuple[ResourceCoordinate, ResourceCoordinate],
    tuple[ResourceCoordinate, ResourceCoordinate],
]:
    # missing registration: extractor holds edge, registry does not
    missing_source = _coord("auditsns", "sub-missing", "subscription")
    missing_target = _coord("auditsqs", "queue-missing", "queue")

    # stale: registry has edge, extractor does not return it
    stale_source = _coord("auditsns", "sub-stale", "subscription")
    stale_target = _coord("auditsqs", "queue-stale", "queue")

    # missing target: edge registered and held, but target is gone
    dangling_source = _coord("auditsns", "sub-dangling", "subscription")
    dangling_target = _coord("auditsqs", "queue-dangling", "queue")

    return (
        (missing_source, missing_target),
        (stale_source, stale_target),
        (dangling_source, dangling_target),
    )


def test_audit_detects_all_three_types(
    stub_adapters: dict[str, object],
) -> None:
    missing_edge, stale_edge, dangling_edge = _edges()

    # Registry: stale + dangling edges (missing edge deliberately absent)
    reference_registry.register(stale_edge[0], stale_edge[1], "Subscription")
    reference_registry.register(dangling_edge[0], dangling_edge[1], "Subscription")

    # Actual models: missing + dangling edges (stale edge deliberately absent)
    stub_adapters["actual_edges"] = [
        (missing_edge[0], missing_edge[1], "Subscription", {}),
        (dangling_edge[0], dangling_edge[1], "Subscription", {}),
    ]
    stub_adapters["target_exists"] = False

    findings = run_audit()

    by_type: dict[str, Inconsistency] = {}
    for finding in findings:
        if finding.type not in by_type:
            by_type[finding.type] = finding
    assert set(by_type) == {
        "missing_registration",
        "stale_registration",
        "missing_target",
    }
    assert by_type["missing_registration"].source == missing_edge[0]
    assert by_type["missing_registration"].target == missing_edge[1]
    assert by_type["stale_registration"].source == stale_edge[0]
    assert by_type["missing_target"].target == dangling_edge[1]
    # Dangling edge is classified once, as missing_target, not also stale.
    dangling_findings = [
        finding for finding in findings if finding.source == dangling_edge[0]
    ]
    assert len(dangling_findings) == 1
    assert dangling_findings[0].type == "missing_target"


def test_audit_filters(
    stub_adapters: dict[str, object],
) -> None:
    missing_edge, stale_edge, dangling_edge = _edges()
    reference_registry.register(stale_edge[0], stale_edge[1], "Subscription")
    reference_registry.register(dangling_edge[0], dangling_edge[1], "Subscription")
    stub_adapters["actual_edges"] = [
        (missing_edge[0], missing_edge[1], "Subscription", {}),
        (dangling_edge[0], dangling_edge[1], "Subscription", {}),
    ]
    stub_adapters["target_exists"] = False

    only_stale = run_audit(AuditFilters(type="stale_registration"))
    assert [f.type for f in only_stale] == ["stale_registration"]

    only_sqs = run_audit(AuditFilters(service="auditsqs"))
    assert all(
        (f.target and f.target.service == "auditsqs")
        or (f.source and f.source.service == "auditsqs")
        for f in only_sqs
    )

    account_filter = run_audit(AuditFilters(account_id="123456789012"))
    assert len(account_filter) >= 1

    region_filter = run_audit(AuditFilters(region="us-east-1"))
    assert len(region_filter) >= 1

    combined = run_audit(AuditFilters(type="missing_registration", service="auditsns"))
    assert [f.type for f in combined] == ["missing_registration"]


def test_audit_does_not_mutate_registry(
    stub_adapters: dict[str, object],
) -> None:
    missing_edge, stale_edge, dangling_edge = _edges()
    reference_registry.register(stale_edge[0], stale_edge[1], "Subscription")
    reference_registry.register(dangling_edge[0], dangling_edge[1], "Subscription")
    stub_adapters["actual_edges"] = [
        (missing_edge[0], missing_edge[1], "Subscription", {}),
        (dangling_edge[0], dangling_edge[1], "Subscription", {}),
    ]

    def _edge_keys() -> set[object]:
        with reference_registry.lock:
            return set(reference_registry._edges)  # noqa: SLF001

    before = _edge_keys()
    assert len(before) == 2  # sanity: the comparison is not vacuous

    run_audit()

    assert _edge_keys() == before


def test_clean_audit(stub_adapters: dict[str, object]) -> None:
    source = _coord("auditsns", "sub-1", "subscription")
    target = _coord("auditsqs", "queue-1", "queue")
    reference_registry.register(source, target, "Subscription")
    stub_adapters["actual_edges"] = [
        (source, target, "Subscription", {}),
    ]
    stub_adapters["target_exists"] = True

    assert run_audit() == []

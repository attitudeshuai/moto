from dataclasses import dataclass
from typing import Any, Literal

from .adapters import adapter_registry
from .coordinates import ResourceCoordinate
from .records import ReferenceRecord
from .registry import reference_registry

InconsistencyType = Literal[
    "missing_registration",
    "stale_registration",
    "missing_target",
]


@dataclass(frozen=True)
class Inconsistency:
    """One reconciliation finding.

    Types:
        missing_registration: a service holds a reference that is not registered;
        stale_registration: a registered edge no longer exists in the source's
            own models (source deleted or repointed);
        missing_target: a registered (and still held) edge points at a target
            that no longer exists in its owning backend.
    """

    type: InconsistencyType
    source: ResourceCoordinate | None
    target: ResourceCoordinate | None
    relation: str | None
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "source": self.source.to_dict() if self.source else None,
            "target": self.target.to_dict() if self.target else None,
            "relation": self.relation,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class AuditFilters:
    type: InconsistencyType | None = None
    service: str | None = None
    account_id: str | None = None
    region: str | None = None


def _edge_matches_coordinate_filters(
    inconsistency: Inconsistency, filters: AuditFilters
) -> bool:
    if filters.type is not None and inconsistency.type != filters.type:
        return False

    coordinates = [
        coordinate
        for coordinate in (inconsistency.source, inconsistency.target)
        if coordinate is not None
    ]
    if filters.service is not None and not any(
        coordinate.service == filters.service for coordinate in coordinates
    ):
        return False
    if filters.account_id is not None and not any(
        coordinate.account_id == filters.account_id for coordinate in coordinates
    ):
        return False
    if filters.region is not None and not any(
        coordinate.region == filters.region for coordinate in coordinates
    ):
        return False
    return True


def _record_sort_key(record: ReferenceRecord) -> tuple[str, ...]:
    """Deterministic, order-comparable key for a registered edge."""
    return (
        record.source.service,
        record.source.account_id or "",
        record.source.region or "",
        record.source.resource_type or "",
        record.source.resource_id,
        record.target.service,
        record.target.account_id or "",
        record.target.region or "",
        record.target.resource_type or "",
        record.target.resource_id,
        record.relation,
    )


def _collect_actual_edges(
    registered: list[ReferenceRecord],
) -> set[tuple[ResourceCoordinate, ResourceCoordinate, str]]:
    """Run all extractors outside the lock and return actual edge identities."""
    actual: set[tuple[ResourceCoordinate, ResourceCoordinate, str]] = set()
    for key in sorted(adapter_registry.extractor_keys):
        extractor = adapter_registry.get_extractor(key)
        if extractor is None:
            continue
        for edge in extractor():
            source, target, relation, _metadata = edge
            actual.add((source, target, relation))
    return actual


def run_audit(filters: AuditFilters | None = None) -> list[Inconsistency]:
    """Reconcile registered edges against service models.

    The audit only reads and reports; it never mutates the registry. Registered
    edges are snapshotted under the registry lock; extractor callbacks and
    existence checkers run outside the lock. Each edge is classified at most
    once (stale > missing-registration > missing-target precedence).
    """
    registry = reference_registry
    with registry.lock:
        registered = list(registry._edges.values())  # noqa: SLF001

    actual = _collect_actual_edges(registered)

    findings: list[Inconsistency] = []

    for record in sorted(registered, key=_record_sort_key):
        edge_identity = (record.source, record.target, record.relation)
        if edge_identity not in actual:
            findings.append(
                Inconsistency(
                    type="stale_registration",
                    source=record.source,
                    target=record.target,
                    relation=record.relation,
                    detail=(
                        "Reference is registered but the source no longer holds "
                        "it in its own models"
                    ),
                )
            )
            continue

        checker = adapter_registry.get_existence_checker(record.target)
        if checker is not None and not checker(record.target):
            findings.append(
                Inconsistency(
                    type="missing_target",
                    source=record.source,
                    target=record.target,
                    relation=record.relation,
                    detail="Referenced object does not exist in its owning backend",
                )
            )

    registered_identities = {
        (record.source, record.target, record.relation) for record in registered
    }
    for source, target, relation in sorted(
        actual,
        key=lambda item: (
            item[0].service,
            item[0].account_id or "",
            item[0].region or "",
            item[0].resource_type or "",
            item[0].resource_id,
            item[1].service,
            item[1].account_id or "",
            item[1].region or "",
            item[1].resource_type or "",
            item[1].resource_id,
            item[2],
        ),
    ):
        if (source, target, relation) not in registered_identities:
            findings.append(
                Inconsistency(
                    type="missing_registration",
                    source=source,
                    target=target,
                    relation=relation,
                    detail=(
                        "Source holds a reference to the target but no edge is "
                        "registered"
                    ),
                )
            )

    if filters is not None:
        findings = [
            finding
            for finding in findings
            if _edge_matches_coordinate_filters(finding, filters)
        ]

    return findings

"""Cross-service reference registry.

This package tracks references between resources owned by different services.

Quick start
-----------

Register a reference when one resource starts pointing at another::

    from moto.core.references import ResourceCoordinate, register_reference

    source = ResourceCoordinate("sns", account, region, "subscription", sub_id)
    target = ResourceCoordinate("sqs", account, region, "queue", queue_name)
    register_reference(source, target, "Subscription")

Reverse-lookup who references an object::

    list_referrers(target)

Configure what happens when a referenced object is deleted or replaced::

    set_service_reference_policy("sqs", PolicyValue.DENY)
    set_account_reference_policy("123456789012", PolicyValue.CASCADE)

Find inconsistencies (missing registrations, stale edges, missing targets)::

    run_audit()

See the individual modules (``coordinates``, ``records``, ``registry``,
``policy``, ``adapters``, ``protocol``, ``warnings``, ``audit``) for details.
"""

from collections.abc import Callable, Mapping
from typing import Any

from .adapters import (
    AdapterRegistry,
    adapter_registry,
)
from .audit import (
    AuditFilters,
    Inconsistency,
    InconsistencyType,
    run_audit,
)
from .coordinates import ResourceCoordinate, coordinate_matches
from .exceptions import ReferenceViolation
from .policy import (
    Operation,
    PolicyBook,
    PolicyResolution,
    PolicyValue,
    policy_book,
)
from .records import (
    EdgeKey,
    ReferenceRecord,
)
from .registry import ReferenceRegistry, reference_registry
from .warnings import WarningLog, WarningRecord, warning_log

__all__ = [
    # coordinates / records
    "ResourceCoordinate",
    "coordinate_matches",
    "ReferenceRecord",
    "EdgeKey",
    # registry
    "ReferenceRegistry",
    "reference_registry",
    "register_reference",
    "unregister_reference",
    "unregister_references_for",
    "replace_reference",
    "list_referrers",
    # policies
    "PolicyValue",
    "PolicyBook",
    "PolicyResolution",
    "policy_book",
    "set_global_reference_policy",
    "set_service_reference_policy",
    "set_account_reference_policy",
    "delete_global_reference_policy",
    "delete_service_reference_policy",
    "delete_account_reference_policy",
    "resolve_reference_policy",
    # protocol
    "guarded_operation",
    "Operation",
    "ReferenceViolation",
    # adapters
    "AdapterRegistry",
    "adapter_registry",
    # audit
    "run_audit",
    "AuditFilters",
    "Inconsistency",
    "InconsistencyType",
    # warnings
    "WarningLog",
    "WarningRecord",
    "warning_log",
    "list_warnings",
    "reset_references",
]


# -- edge convenience wrappers ------------------------------------------------


def register_reference(
    source: ResourceCoordinate,
    target: ResourceCoordinate,
    relation: str,
    metadata: Mapping[str, Any] | None = None,
) -> ReferenceRecord:
    """Record that ``source`` references ``target`` (idempotent)."""
    return reference_registry.register(source, target, relation, metadata)


def unregister_reference(
    source: ResourceCoordinate,
    target: ResourceCoordinate,
    relation: str,
) -> bool:
    """Remove a single registered reference edge."""
    return reference_registry.unregister(source, target, relation)


def unregister_references_for(source: ResourceCoordinate) -> int:
    """Remove every reference edge originating from ``source``."""
    return reference_registry.unregister_source(source)


def replace_reference(
    source: ResourceCoordinate,
    old_target: ResourceCoordinate,
    new_target: ResourceCoordinate,
    relation: str,
    metadata: Mapping[str, Any] | None = None,
) -> ReferenceRecord:
    """Atomically move a reference edge from ``old_target`` to ``new_target``."""
    return reference_registry.replace_target(
        source, old_target, new_target, relation, metadata
    )


def list_referrers(
    target: ResourceCoordinate,
    source_service: str | None = None,
    source_account_id: str | None = None,
    source_region: str | None = None,
    relation: str | None = None,
) -> list[ReferenceRecord]:
    """Reverse-lookup every resource referencing ``target``."""
    return reference_registry.list_referrers(
        target,
        source_service=source_service,
        source_account_id=source_account_id,
        source_region=source_region,
        relation=relation,
    )


# -- policy convenience wrappers ----------------------------------------------


def set_global_reference_policy(policy: PolicyValue) -> None:
    policy_book.set_global_policy(policy)


def set_service_reference_policy(
    service: str,
    policy: PolicyValue,
    resource_type: str | None = None,
) -> None:
    policy_book.set_service_policy(service, policy, resource_type)


def set_account_reference_policy(
    account_id: str,
    policy: PolicyValue,
    service: str | None = None,
) -> None:
    policy_book.set_account_policy(account_id, policy, service)


def delete_global_reference_policy() -> bool:
    return policy_book.delete_global_policy()


def delete_service_reference_policy(
    service: str, resource_type: str | None = None
) -> bool:
    return policy_book.delete_service_policy(service, resource_type)


def delete_account_reference_policy(
    account_id: str, service: str | None = None
) -> bool:
    return policy_book.delete_account_policy(account_id, service)


def resolve_reference_policy(
    target: ResourceCoordinate,
    operation: Operation = "delete",
) -> PolicyResolution:
    """Return the policy that would apply to ``target`` for ``operation``."""
    return policy_book.resolve(target, operation)


# -- protocol / warnings / reset ----------------------------------------------


def guarded_operation(
    target: ResourceCoordinate,
    action: Callable[[], Any],
    operation: Operation = "delete",
    cascade_depth: int | None = None,
) -> Any:
    """Run an action (delete/replace of ``target``) under reference policy."""
    from .protocol import guarded_operation as _guarded

    if cascade_depth is None:
        return _guarded(target, action, operation)
    return _guarded(target, action, operation, cascade_depth=cascade_depth)


def list_warnings(
    target: ResourceCoordinate | None = None,
) -> list[WarningRecord]:
    """Return warnings emitted by warn-policy operations."""
    return warning_log.list_warnings(target)


def reset_references() -> None:
    """Reset every mutable piece of the reference subsystem.

    Clears registered edges, policies, warnings and tombstones. Code-time
    adapter registrations are preserved.
    """
    warning_log.reset()
    policy_book.reset()
    reference_registry.reset()

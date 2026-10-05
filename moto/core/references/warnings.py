from dataclasses import dataclass
from typing import Any

from .coordinates import ResourceCoordinate
from .records import ReferenceRecord, utc_now_iso
from .registry import reference_registry


@dataclass(frozen=True)
class WarningRecord:
    """A warning emitted when a warn-policy operation is allowed to proceed."""

    target: ResourceCoordinate
    operation: str
    reason: str
    references: tuple[ReferenceRecord, ...]
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "target": self.target.to_dict(),
            "operation": self.operation,
            "reason": self.reason,
            "references": [record.to_dict() for record in self.references],
            "created_at": self.created_at,
        }


class WarningLog:
    """Stores warn-policy warnings so they are queryable, not silent."""

    def __init__(self) -> None:
        self._warnings: list[WarningRecord] = []

    def add(
        self,
        target: ResourceCoordinate,
        operation: str,
        references: list[ReferenceRecord],
        reason: str = "policy_warn",
    ) -> WarningRecord:
        record = WarningRecord(
            target=target,
            operation=operation,
            reason=reason,
            references=tuple(references),
            created_at=utc_now_iso(),
        )
        with reference_registry.lock:
            self._warnings.append(record)
        return record

    def list_warnings(
        self, target: ResourceCoordinate | None = None
    ) -> list[WarningRecord]:
        with reference_registry.lock:
            snapshot = list(self._warnings)
        if target is None:
            return snapshot
        return [record for record in snapshot if record.target == target]

    def reset(self) -> None:
        with reference_registry.lock:
            self._warnings.clear()


warning_log = WarningLog()

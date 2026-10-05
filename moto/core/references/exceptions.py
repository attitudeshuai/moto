from typing import TYPE_CHECKING

from moto.core.exceptions import ServiceException

if TYPE_CHECKING:
    from .coordinates import ResourceCoordinate
    from .records import ReferenceRecord


class ReferenceViolation(ServiceException):
    """Raised when an operation conflicts with registered references.

    Used for: explicit ``deny`` policies, aborted cascades (missing deleter
    adapter, cycle, depth limit), and attempts to register a reference onto a
    target that is currently being deleted.

    Attributes:
        target: Coordinate of the referenced object involved.
        reason: Machine-readable cause (``policy_deny``, ``cascade_impossible``,
            ``cascade_cycle``, ``cascade_depth``, ``target_deleting``, ...).
        references: Snapshot of the references involved at decision time.
    """

    code = "ReferenceViolation"

    def __init__(
        self,
        message: str,
        *,
        target: "ResourceCoordinate | None" = None,
        reason: str = "policy_deny",
        references: "list[ReferenceRecord] | None" = None,
    ) -> None:
        super().__init__(message)
        self.target = target
        self.reason = reason
        self.references = list(references or [])

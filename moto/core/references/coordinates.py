from dataclasses import dataclass
from typing import Any

from moto.utilities.arns import Arn, parse_arn


@dataclass(frozen=True)
class ResourceCoordinate:
    """Fully qualified identity of a resource across service boundaries.

    Attributes:
        service: The moto/botocore service name owning the resource, e.g. ``sqs``.
        account_id: The account that owns the resource. ``None`` is only used in
            query patterns to express a wildcard.
        region: The region the resource lives in. Global services use their
            partition/global convention. ``None`` is only used in query patterns.
        resource_type: The type of the resource within the service, e.g. ``queue``.
        resource_id: The unique identifier of the resource.
    """

    service: str
    account_id: str | None
    region: str | None
    resource_type: str | None
    resource_id: str

    @classmethod
    def from_arn(cls, arn: str) -> "ResourceCoordinate":
        """Create a coordinate from a resource ARN.

        Mirrors :func:`moto.utilities.arns.parse_arn`; empty ARN components are
        normalized to ``None``.
        """
        parsed: Arn = parse_arn(arn)
        return cls(
            service=parsed.service,
            account_id=parsed.account or None,
            region=parsed.region or None,
            resource_type=parsed.resource_type,
            resource_id=parsed.resource_id,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "service": self.service,
            "account_id": self.account_id,
            "region": self.region,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
        }

    def matches(self, pattern: "ResourceCoordinate") -> bool:
        """Return whether this coordinate matches a (possibly partial) pattern.

        Every non-``None`` field of ``pattern`` must be equal; ``None`` fields
        are treated as wildcards.
        """
        return coordinate_matches(self, pattern)


def coordinate_matches(
    coordinate: ResourceCoordinate, pattern: ResourceCoordinate
) -> bool:
    """Field-wise match: non-``None`` pattern fields must be equal."""
    for field_name in (
        "service",
        "account_id",
        "region",
        "resource_type",
        "resource_id",
    ):
        expected = getattr(pattern, field_name)
        if expected is not None and getattr(coordinate, field_name) != expected:
            return False
    return True


def coordinate_sort_key(
    coordinate: ResourceCoordinate,
) -> tuple[str, str, str, str, str]:
    """Deterministic, order-comparable key for a coordinate."""
    return (
        coordinate.service,
        coordinate.account_id or "",
        coordinate.region or "",
        coordinate.resource_type or "",
        coordinate.resource_id,
    )

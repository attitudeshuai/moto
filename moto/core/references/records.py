from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .coordinates import ResourceCoordinate

# Normalized metadata: sorted key/value pairs, hashable and JSON-friendly.
Metadata = tuple[tuple[str, Any], ...]


def normalize_metadata(metadata: Mapping[str, Any] | None) -> Metadata:
    if not metadata:
        return ()
    return tuple((key, metadata[key]) for key in sorted(metadata))


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@dataclass(frozen=True)
class ReferenceRecord:
    """A single directed reference edge.

    The identity of an edge is the triple (source, target, relation). The
    registration timestamp and metadata describe the edge but are not part of
    its identity.
    """

    source: ResourceCoordinate
    target: ResourceCoordinate
    relation: str
    registered_at: str
    metadata: Metadata = field(default=())

    @property
    def key(self) -> tuple[ResourceCoordinate, ResourceCoordinate, str]:
        return (self.source, self.target, self.relation)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source.to_dict(),
            "target": self.target.to_dict(),
            "relation": self.relation,
            "registered_at": self.registered_at,
            "metadata": dict(self.metadata),
        }


EdgeKey = tuple[ResourceCoordinate, ResourceCoordinate, str]

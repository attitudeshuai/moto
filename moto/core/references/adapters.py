from collections.abc import Callable, Iterable
from typing import Any

from .coordinates import ResourceCoordinate

# A deleter removes the resource at the given coordinate (cascade policy).
Deleter = Callable[[ResourceCoordinate], None]
# An existence checker returns whether the target resource still exists.
ExistenceChecker = Callable[[ResourceCoordinate], bool]
# An extractor yields edges (source, target, relation, metadata) currently held
# by the service's own models. Plain tuples are returned; the audit converts
# them to registered edge identities.
ExtractedEdge = tuple[ResourceCoordinate, ResourceCoordinate, str, dict[str, Any]]
Extractor = Callable[[], Iterable[ExtractedEdge]]

AdapterKey = tuple[str, str]


class AdapterRegistry:
    """Code-time registry of per-resource-type service adapters.

    Services register adapters when their modules are imported; registrations
    are intentionally *not* removed by the data reset, only by explicit
    deletion.
    """

    def __init__(self) -> None:
        self._deleters: dict[AdapterKey, Deleter] = {}
        self._existence_checkers: dict[AdapterKey, ExistenceChecker] = {}
        self._extractors: dict[AdapterKey, Extractor] = {}

    # -- deleters -----------------------------------------------------------

    def register_deleter(
        self, service: str, resource_type: str, deleter: Deleter
    ) -> None:
        self._deleters[(service, resource_type)] = deleter

    def get_deleter(self, coordinate: ResourceCoordinate) -> Deleter | None:
        if coordinate.resource_type is None:
            return None
        return self._deleters.get((coordinate.service, coordinate.resource_type))

    # -- existence checkers -------------------------------------------------

    def register_existence_checker(
        self,
        service: str,
        resource_type: str,
        checker: ExistenceChecker,
    ) -> None:
        self._existence_checkers[(service, resource_type)] = checker

    def get_existence_checker(
        self, coordinate: ResourceCoordinate
    ) -> ExistenceChecker | None:
        if coordinate.resource_type is None:
            return None
        return self._existence_checkers.get(
            (coordinate.service, coordinate.resource_type)
        )

    # -- extractors ---------------------------------------------------------

    def register_extractor(
        self, service: str, resource_type: str, extractor: Extractor
    ) -> None:
        self._extractors[(service, resource_type)] = extractor

    @property
    def extractor_keys(self) -> set[AdapterKey]:
        return set(self._extractors)

    def get_extractor(self, key: AdapterKey) -> Extractor | None:
        return self._extractors.get(key)

    # -- maintenance --------------------------------------------------------

    def clear(self) -> None:
        """Remove every adapter registration (mainly used in tests)."""
        self._deleters.clear()
        self._existence_checkers.clear()
        self._extractors.clear()

    def describe(self) -> dict[str, Any]:
        return {
            "deleters": sorted(self._deleters),
            "existence_checkers": sorted(self._existence_checkers),
            "extractors": sorted(self._extractors),
        }


adapter_registry = AdapterRegistry()

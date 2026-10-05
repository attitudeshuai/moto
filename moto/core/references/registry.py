from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from threading import Condition, RLock
from typing import Any

from .adapters import adapter_registry
from .coordinates import ResourceCoordinate, coordinate_matches
from .exceptions import ReferenceViolation
from .records import (
    EdgeKey,
    ReferenceRecord,
    normalize_metadata,
    utc_now_iso,
)


class ReferenceRegistry:
    """In-memory registry of directed cross-service reference edges.

    Maintains a forward index (source coordinate -> edges) and a reverse index
    (target coordinate -> edges). Every mutation is performed while holding the
    registry lock, which is shared with the guarded deletion protocol and the
    audit machinery, so all modules in this package have one lock order.

    Registration is two-phase: callers announce a *pending intent* for the
    target before checking its existence (the existence checker callback runs
    outside the lock), then finish under the lock. A guarded deletion waits for
    pending intents on its target before tombstoning it, so a registration can
    never race a deletion into a contradictory state.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._condition = Condition(self._lock)
        self._edges: dict[EdgeKey, ReferenceRecord] = {}
        self._by_source: dict[ResourceCoordinate, set[EdgeKey]] = {}
        self._by_target: dict[ResourceCoordinate, set[EdgeKey]] = {}
        # Coordinates currently inside a guarded delete/replace protocol.
        self._tombstones: set[ResourceCoordinate] = set()
        # Coordinates with a registration currently in progress.
        self._pending: dict[ResourceCoordinate, int] = {}

    @property
    def lock(self) -> RLock:
        return self._lock

    def reset(self) -> None:
        """Remove all registered edges, tombstones and pending intents.

        Adapter registrations live outside the mutable data and are not reset.
        """
        with self._condition:
            self._edges.clear()
            self._by_source.clear()
            self._by_target.clear()
            self._tombstones.clear()
            self._pending.clear()
            self._condition.notify_all()

    def is_tombstoned(self, coordinate: ResourceCoordinate) -> bool:
        with self._lock:
            return coordinate in self._tombstones

    def add_tombstones(self, coordinates: set[ResourceCoordinate]) -> None:
        with self._lock:
            self._tombstones.update(coordinates)

    def remove_tombstones(self, coordinates: set[ResourceCoordinate]) -> None:
        with self._lock:
            self._tombstones.difference_update(coordinates)

    def wait_for_pending(self, target: ResourceCoordinate) -> None:
        """Block until no registration intent is pending on ``target``."""
        with self._condition:
            self._condition.wait_for(lambda: target not in self._pending)

    def snapshot_referrers(self, target: ResourceCoordinate) -> list[ReferenceRecord]:
        """Return a copied snapshot of edges matching ``target`` (lock held)."""
        return list(self.list_referrers(target))

    # -- intent bookkeeping --------------------------------------------------

    def _begin_intent(self, target: ResourceCoordinate) -> None:
        with self._condition:
            self._pending[target] = self._pending.get(target, 0) + 1

    def _end_intent(self, target: ResourceCoordinate) -> None:
        with self._condition:
            count = self._pending[target] - 1
            if count > 0:
                self._pending[target] = count
            else:
                del self._pending[target]
            self._condition.notify_all()

    @contextmanager
    def _registration_intent(
        self, target: ResourceCoordinate
    ) -> Iterator[
        tuple[
            Callable[[ResourceCoordinate], bool] | None,
            bool | None,
        ]
    ]:
        with self._lock:
            if target in self._tombstones:
                raise ReferenceViolation(
                    f"Cannot register reference: {target} is being deleted",
                    target=target,
                    reason="target_deleting",
                )
        self._begin_intent(target)
        try:
            checker = adapter_registry.get_existence_checker(target)
            exists = checker(target) if checker is not None else None
            yield checker, exists
        finally:
            self._end_intent(target)

    # -- registration --------------------------------------------------------

    def register(
        self,
        source: ResourceCoordinate,
        target: ResourceCoordinate,
        relation: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> ReferenceRecord:
        """Register a reference edge.

        Fails with :class:`ReferenceViolation` when the target is being deleted
        (``target_deleting``) or when a registered existence checker reports the
        target absent (``target_nonexistent``). Registration is idempotent: the
        same (source, target, relation) keeps its original timestamp and the
        metadata is replaced when provided.
        """
        key: EdgeKey = (source, target, relation)

        with self._registration_intent(target) as intent:
            checker, exists = intent

        with self._lock:
            if target in self._tombstones:
                raise ReferenceViolation(
                    f"Cannot register reference: {target} is being deleted",
                    target=target,
                    reason="target_deleting",
                )
            if checker is not None and exists is False:
                raise ReferenceViolation(
                    f"Cannot register reference: {target} does not exist",
                    target=target,
                    reason="target_nonexistent",
                )

            existing = self._edges.get(key)
            if existing is not None:
                if metadata is not None:
                    existing = ReferenceRecord(
                        source=source,
                        target=target,
                        relation=relation,
                        registered_at=existing.registered_at,
                        metadata=normalize_metadata(metadata),
                    )
                    self._edges[key] = existing
                return existing
            record = ReferenceRecord(
                source=source,
                target=target,
                relation=relation,
                registered_at=utc_now_iso(),
                metadata=normalize_metadata(metadata),
            )
            self._edges[key] = record
            self._by_source.setdefault(source, set()).add(key)
            self._by_target.setdefault(target, set()).add(key)
            return record

    def unregister(
        self,
        source: ResourceCoordinate,
        target: ResourceCoordinate,
        relation: str,
    ) -> bool:
        """Remove a single edge. Returns whether an edge was removed."""
        key = (source, target, relation)
        with self._lock:
            return self._remove_edge(key)

    def unregister_source(self, source: ResourceCoordinate) -> int:
        """Remove every edge originating from ``source``. Returns the count."""
        with self._lock:
            keys = list(self._by_source.get(source, ()))
            for key in keys:
                self._remove_edge(key)
            return len(keys)

    def replace_target(
        self,
        source: ResourceCoordinate,
        old_target: ResourceCoordinate,
        new_target: ResourceCoordinate,
        relation: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> ReferenceRecord:
        """Atomically move an edge from ``old_target`` to ``new_target``.

        Raises:
            KeyError: if the (source, old_target, relation) edge is missing.
            ValueError: if an (source, new_target, relation) edge already exists.
            ReferenceViolation: if either target is being deleted, or the new
                target does not exist according to its existence checker.
        """
        old_key: EdgeKey = (source, old_target, relation)
        new_key: EdgeKey = (source, new_target, relation)

        with self._lock:
            if old_key not in self._edges:
                raise KeyError(
                    f"Cannot replace reference: edge {old_key} is not registered"
                )
            if new_key in self._edges:
                raise ValueError(
                    f"Cannot replace reference: edge {new_key} already exists"
                )
            if old_target in self._tombstones or new_target in self._tombstones:
                raise ReferenceViolation(
                    "Cannot replace reference: a target is being deleted",
                    target=new_target,
                    reason="target_deleting",
                )

        with self._registration_intent(new_target) as intent:
            checker, exists = intent

        with self._lock:
            if old_target in self._tombstones or new_target in self._tombstones:
                raise ReferenceViolation(
                    "Cannot replace reference: a target is being deleted",
                    target=new_target,
                    reason="target_deleting",
                )
            if checker is not None and exists is False:
                raise ReferenceViolation(
                    f"Cannot replace reference: {new_target} does not exist",
                    target=new_target,
                    reason="target_nonexistent",
                )

            old_record = self._edges.pop(old_key)
            self._by_source[source].discard(old_key)
            self._by_target[old_target].discard(old_key)
            record = ReferenceRecord(
                source=source,
                target=new_target,
                relation=relation,
                registered_at=old_record.registered_at,
                metadata=normalize_metadata(
                    metadata if metadata is not None else dict(old_record.metadata)
                ),
            )
            self._edges[new_key] = record
            self._by_source.setdefault(source, set()).add(new_key)
            self._by_target.setdefault(new_target, set()).add(new_key)
            return record

    def list_referrers(
        self,
        target: ResourceCoordinate,
        source_service: str | None = None,
        source_account_id: str | None = None,
        source_region: str | None = None,
        relation: str | None = None,
        limit: int | None = None,
    ) -> list[ReferenceRecord]:
        """Return edges pointing at resources matching the target pattern.

        ``target`` may be partial: ``None`` fields are wildcards, so a queue can
        be looked up across accounts/regions. The ``source_*`` parameters filter
        referrers; ``relation`` filters the relation type; ``limit`` truncates
        the (sorted) result.
        """
        with self._lock:
            candidates: list[ReferenceRecord] = [
                record
                for coordinate, keys in self._by_target.items()
                if coordinate_matches(coordinate, target)
                for record in (self._edges[key] for key in keys)
            ]

        def _matches(record: ReferenceRecord) -> bool:
            if relation is not None and record.relation != relation:
                return False
            if source_service is not None and record.source.service != source_service:
                return False
            if (
                source_account_id is not None
                and record.source.account_id != source_account_id
            ):
                return False
            if source_region is not None and record.source.region != source_region:
                return False
            return True

        results = [record for record in candidates if _matches(record)]
        results.sort(
            key=lambda record: (
                record.source.service,
                record.source.account_id or "",
                record.source.region or "",
                record.source.resource_type or "",
                record.source.resource_id,
                record.relation,
            )
        )
        if limit is not None:
            results = results[:limit]
        return results

    def _remove_edge(self, key: EdgeKey) -> bool:
        record = self._edges.pop(key, None)
        if record is None:
            return False
        source_keys = self._by_source.get(record.source)
        if source_keys is not None:
            source_keys.discard(key)
            if not source_keys:
                del self._by_source[record.source]
        target_keys = self._by_target.get(record.target)
        if target_keys is not None:
            target_keys.discard(key)
            if not target_keys:
                del self._by_target[record.target]
        return True


# Process-wide singleton.
reference_registry = ReferenceRegistry()

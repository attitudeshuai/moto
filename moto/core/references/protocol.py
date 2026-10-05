from collections.abc import Callable
from typing import TypeVar

from .adapters import adapter_registry
from .coordinates import ResourceCoordinate, coordinate_sort_key
from .exceptions import ReferenceViolation
from .policy import Operation, PolicyValue, policy_book
from .records import ReferenceRecord
from .registry import reference_registry
from .warnings import warning_log

T = TypeVar("T")

DEFAULT_CASCADE_DEPTH = 16

# An edge u -> v within the cascade graph (u references v).
_GraphEdge = tuple[ResourceCoordinate, ResourceCoordinate]


class _CascadePlan:
    """Snapshotted plan for a cascade deletion.

    The graph contains every reverse edge discovered by walking the registry
    from the original target. ``order`` lists referencing resources leaves-first
    (each node is preceded by everything that references it), which is exactly
    the order deleters must run in.
    """

    def __init__(
        self,
        target: ResourceCoordinate,
        nodes: set[ResourceCoordinate],
        edges: set[_GraphEdge],
        order: list[ResourceCoordinate],
    ) -> None:
        self.target = target
        self.nodes = nodes
        self.edges = edges
        self.order = order

    @property
    def all_coordinates(self) -> set[ResourceCoordinate]:
        return set(self.nodes)


def _build_cascade_plan(target: ResourceCoordinate, max_depth: int) -> _CascadePlan:
    """Collect the reverse-edge closure and derive a leaves-first order.

    Must be called while holding the registry lock; performs index reads only.
    Cycles are detected with a Kahn pass over the graph, so a DAG whose nodes
    are reachable at unequal depths is not mistaken for a cycle.
    """
    nodes: set[ResourceCoordinate] = set()
    edges: set[_GraphEdge] = set()
    frontier = {target}
    depth = 0

    while frontier:
        depth += 1
        if depth > max_depth:
            raise ReferenceViolation(
                f"Cascade depth limit ({max_depth}) exceeded while collecting "
                f"references to {target}",
                target=target,
                reason="cascade_depth",
            )
        next_frontier: set[ResourceCoordinate] = set()
        for coordinate in frontier:
            for record in reference_registry.snapshot_referrers(coordinate):
                source = record.source
                edges.add((source, coordinate))
                if source not in nodes:
                    nodes.add(source)
                    next_frontier.add(source)
        frontier = next_frontier

    # Kahn ordering: indegree counts edges w -> u (other nodes referencing u).
    # Nodes with indegree zero are leaves and delete first.
    indegree: dict[ResourceCoordinate, int] = dict.fromkeys(nodes, 0)
    outgoing: dict[ResourceCoordinate, set[ResourceCoordinate]] = {
        node: set() for node in nodes
    }
    for source, target_node in edges:
        if source in indegree and target_node in indegree:
            indegree[target_node] += 1
            outgoing[source].add(target_node)

    ready = sorted(
        (node for node, degree in indegree.items() if degree == 0),
        key=coordinate_sort_key,
    )
    order: list[ResourceCoordinate] = []
    while ready:
        node = ready.pop(0)
        order.append(node)
        for next_node in sorted(outgoing[node], key=coordinate_sort_key):
            indegree[next_node] -= 1
            if indegree[next_node] == 0:
                ready.append(next_node)
        ready.sort(key=coordinate_sort_key)

    return _CascadePlan(target=target, nodes=nodes, edges=edges, order=order)


def _cycle_records(plan: _CascadePlan) -> list[ReferenceRecord]:
    ordered = set(plan.order)
    unresolved = plan.nodes - ordered
    return [
        record
        for coordinate in unresolved
        for record in reference_registry.snapshot_referrers(coordinate)
        if record.source in unresolved
    ]


def _validate_cascade_plan(plan: _CascadePlan) -> None:
    """Pre-flight validation used before tombstoning the cascade closure."""
    unresolved = plan.nodes - set(plan.order)
    if unresolved:
        raise ReferenceViolation(
            f"Cannot cascade-delete references to {plan.target}: cyclic "
            f"references detected",
            target=plan.target,
            reason="cascade_cycle",
            references=_cycle_records(plan),
        )
    for coordinate in plan.all_coordinates:
        if coordinate.account_id is None or coordinate.region is None:
            raise ReferenceViolation(
                f"Cannot cascade-delete {coordinate}: incomplete coordinate",
                target=plan.target,
                reason="cascade_impossible",
            )
        if adapter_registry.get_deleter(coordinate) is None:
            raise ReferenceViolation(
                f"Cannot cascade-delete references to {plan.target}: no deleter "
                f"registered for referencing resource {coordinate}",
                target=plan.target,
                reason="cascade_impossible",
            )


def _execute_cascade(
    plan: _CascadePlan,
    tombstones: set[ResourceCoordinate],
) -> None:
    # Deleters run outside the registry lock in the validated leaves-first
    # order; only edge bookkeeping takes the lock.
    for coordinate in plan.order:
        deleter = adapter_registry.get_deleter(coordinate)
        if deleter is None:
            raise ReferenceViolation(
                f"Cannot cascade-delete {coordinate}: no deleter registered",
                target=plan.target,
                reason="cascade_impossible",
            )
        try:
            deleter(coordinate)
        except BaseException:
            reference_registry.remove_tombstones(tombstones)
            raise
        reference_registry.unregister_source(coordinate)


def guarded_operation(
    target: ResourceCoordinate,
    action: Callable[[], T],
    operation: Operation = "delete",
    cascade_depth: int = DEFAULT_CASCADE_DEPTH,
) -> T:
    """Run ``action`` (delete/replace of ``target``) under reference policy.

    Protocol:
      1. under the lock, wait for pending registration intents on ``target``,
         tombstone it, and snapshot its incoming edges;
      2. resolve the effective policy;
      3. deny -> abort with the reference list; warn -> record a warning;
         cascade -> build/validate the closure, tombstone it, delete referrers
         leaves-first via registered deleters;
      4. run ``action``; on failure clear tombstones and re-raise;
      5. on success clear tombstones. For passive/warn operations the edges are
         intentionally retained, so the audit exposes them as missing targets
         rather than silently dropping the knowledge.
    """
    registry = reference_registry

    with registry.lock:
        if target in registry._tombstones:  # noqa: SLF001
            raise ReferenceViolation(
                f"Cannot operate on {target}: already being deleted or replaced",
                target=target,
                reason="target_deleting",
            )
        # Wait for registrations in progress so the tombstone/snapshot is
        # consistent with their outcome (Condition wait releases this lock).
        registry.wait_for_pending(target)
        registry.add_tombstones({target})
        snapshot = registry.snapshot_referrers(target)
        resolution = policy_book.resolve(target, operation)
        policy = resolution.policy

    tombstones: set[ResourceCoordinate] = {target}

    try:
        if policy is PolicyValue.DENY:
            raise ReferenceViolation(
                f"Cannot {operation} {target}: it is referenced by "
                f"{len(snapshot)} resource(s)",
                target=target,
                reason="policy_deny",
                references=snapshot,
            )

        if policy is PolicyValue.WARN:
            warning_log.add(target, operation, snapshot)

        if policy is PolicyValue.CASCADE:
            with registry.lock:
                plan = _build_cascade_plan(target, max_depth=cascade_depth)
                # Validate before tombstoning the closure, so an impossible
                # cascade leaves only the target tombstone behind.
                _validate_cascade_plan(plan)
                closure_tombstones = plan.all_coordinates
                registry.add_tombstones(closure_tombstones)
            tombstones.update(closure_tombstones)
            _execute_cascade(plan, tombstones)

        result = action()
    except BaseException:
        registry.remove_tombstones(tombstones)
        raise
    else:
        if policy is PolicyValue.CASCADE:
            # Guarantee no residual edges onto a successfully deleted target.
            with registry.lock:
                for record in registry.snapshot_referrers(target):
                    registry.unregister(record.source, record.target, record.relation)
        registry.remove_tombstones(tombstones)
        return result

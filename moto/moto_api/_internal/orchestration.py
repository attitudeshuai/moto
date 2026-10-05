from typing import Any

from .managed_state_model import ManagedState, OrchestrationError


class OrchestrationPlan:
    """
    Progress a group of :class:`ManagedState` resources in an explicitly
    declared dependency order.

    .. sourcecode:: python

        plan = OrchestrationPlan()
        plan.add(network, target="available")
        plan.add(instance, target="available", depends_on=[network])
        plan.add(task, target="running", depends_on=[instance])
        plan.execute()

    A resource is only progressed once all of its dependencies have reached
    their target. If a dependency fails (a failure was injected) or can not
    reach its target, the dependent resources are not progressed and an
    :class:`OrchestrationError` is raised.

    The plan (and everything it records) lives in memory only - it is reset
    together with the Moto backends and is never persisted.
    """

    def __init__(self) -> None:
        self._nodes: dict[int, dict[str, Any]] = {}
        self._order: list[int] = []

    def add(
        self,
        resource: ManagedState,
        target: str | None = None,
        depends_on: list[ManagedState] | None = None,
    ) -> "OrchestrationPlan":
        """
        Add a resource to the plan.

        :param resource: the resource to progress
        :param target: the status the resource should end in. ``None``
                       progresses the resource to its final status.
        :param depends_on: resources that must reach their target before this
                           resource is progressed.
        """
        if not isinstance(resource, ManagedState):
            raise TypeError(f"Not a ManagedState resource: {resource!r}")
        key = id(resource)
        if key in self._nodes:
            raise ValueError("Resource was already added to this plan")
        dependencies = []
        for dependency in depends_on or []:
            if not isinstance(dependency, ManagedState):
                raise TypeError(f"Not a ManagedState resource: {dependency!r}")
            dependencies.append(id(dependency))
        self._nodes[key] = {
            "resource": resource,
            "target": target,
            "dependencies": dependencies,
        }
        self._order.append(key)
        return self

    @classmethod
    def chain(
        cls,
        resources: list[ManagedState],
        target: list[str | None] | str | None = None,
    ) -> "OrchestrationPlan":
        """
        Create a plan where every resource depends on the one before it, so the
        resources are progressed strictly in the given order.

        ``target`` is a list of target statuses (one per resource) or a single
        target used for every resource. ``None`` progresses every resource to
        its final status.
        """
        if isinstance(target, str) or target is None:
            resolved_targets: list[str | None] = [target] * len(resources)
        else:
            if len(target) != len(resources):
                raise ValueError("The number of targets must match the resources")
            resolved_targets = list(target)

        plan = cls()
        for index, resource in enumerate(resources):
            plan.add(
                resource,
                target=resolved_targets[index],
                depends_on=resources[:index],
            )
        return plan

    def execute(
        self, trigger: str = ManagedState.TRIGGER_ORCHESTRATION
    ) -> dict[ManagedState, str | None]:
        """
        Execute the plan in dependency order.

        Returns a mapping of ``resource -> resulting status`` for every
        progressed resource. Raises :class:`OrchestrationError` if the plan has
        a cycle, references an unknown dependency, or one of the resources
        failed or could not reach its target.
        """
        ordered_keys = self._topological_order()
        results: dict[Any, str | None] = {}
        failed: set[int] = set()
        blocked: set[int] = set()

        for key in ordered_keys:
            node = self._nodes[key]
            resource = node["resource"]
            if any(dep in failed or dep in blocked for dep in node["dependencies"]):
                blocked.add(key)
                continue
            try:
                results[resource] = resource.advance_to(node["target"], trigger=trigger)
            except OrchestrationError:
                # Keep the status the resource failed in, so the partial
                # results remain observable.
                results[resource] = resource.status
                failed.add(key)

        if failed or blocked:
            failed_resources = [
                self._nodes[key]["resource"] for key in ordered_keys if key in failed
            ]
            blocked_resources = [
                self._nodes[key]["resource"] for key in ordered_keys if key in blocked
            ]
            message = "Orchestration did not complete: "
            if failed_resources:
                message += (
                    f"{len(failed_resources)} resource(s) failed "
                    f"({', '.join(r.model_name for r in failed_resources)})"
                )
            if blocked_resources:
                if failed_resources:
                    message += "; "
                message += (
                    f"{len(blocked_resources)} resource(s) were not progressed "
                    "because a dependency did not complete"
                )
            raise OrchestrationError(message, results=results)

        return results

    def _topological_order(self) -> list[int]:
        """Kahn's algorithm, preserving insertion order for determinism."""
        in_degree = {
            key: len([dep for dep in node["dependencies"] if dep in self._nodes])
            for key, node in self._nodes.items()
        }
        for node in self._nodes.values():
            for dep in node["dependencies"]:
                if dep not in self._nodes:
                    resource = node["resource"]
                    raise OrchestrationError(
                        f"'{resource.model_name}' depends on a resource that was "
                        "not added to the plan"
                    )

        ordered: list[int] = []
        ready = [key for key in self._order if in_degree[key] == 0]
        while ready:
            key = ready.pop(0)
            ordered.append(key)
            for candidate in self._order:
                if key in self._nodes[candidate]["dependencies"]:
                    in_degree[candidate] -= 1
                    if in_degree[candidate] == 0:
                        ready.append(candidate)

        if len(ordered) != len(self._nodes):
            raise OrchestrationError(
                "Can not orchestrate resources with cyclic dependencies"
            )
        return ordered

import threading

import pytest

from moto.moto_api import OrchestrationError, OrchestrationPlan, state_manager
from moto.moto_api._internal.managed_state_model import ManagedState


class TrackedModel(ManagedState):
    """Model that records every status change for later assertions."""

    def __init__(
        self,
        model_name: str = "example::tracked",
        transitions: list | None = None,
        failure_status=None,
        failure_reason_attr=None,
    ):
        super().__init__(
            model_name,
            transitions
            or [
                ("first", "second"),
                ("second", "third"),
                ("third", "fourth"),
                ("fourth", "fifth"),
            ],
            failure_status=failure_status,
            failure_reason_attr=failure_reason_attr,
        )
        self.moves: list[tuple] = []
        self.reason: str | None = None

    def _on_status_change(self, old, new) -> None:
        self.moves.append((old, new))


@pytest.fixture(autouse=True)
def reset_example_transition():
    state_manager.set_transition(
        model_name="example::tracked",
        transition={"progression": "manual", "times": 1},
    )
    state_manager.set_transition(
        model_name="example::cyclic",
        transition={"progression": "manual", "times": 1},
    )
    yield
    state_manager.unset_transition("example::tracked")
    state_manager.unset_transition("example::cyclic")


# ---------------------------------------------------------------------------
# advance_to - force a resource to a target stage
# ---------------------------------------------------------------------------
def test_advance_to_target_status_in_one_call():
    model = TrackedModel()

    assert model.status == "first"

    result = model.advance_to("third")

    assert result == "third"
    assert model.status == "third"
    assert model.last_trigger == "orchestration"
    # Every intermediate stage was visited exactly once
    assert model.moves == [
        ("first", "second"),
        ("second", "third"),
    ]


def test_advance_to_without_target_goes_to_the_final_status():
    model = TrackedModel()

    model.advance_to(None)

    assert model.status == "fifth"
    assert model.moves == [
        ("first", "second"),
        ("second", "third"),
        ("third", "fourth"),
        ("fourth", "fifth"),
    ]


def test_advance_to_ignores_configured_progression():
    # Even with a huge manual threshold, an orchestrated step is taken directly
    state_manager.set_transition(
        "example::tracked", {"progression": "manual", "times": 999}
    )
    model = TrackedModel()

    model.advance_to("fourth")

    assert model.status == "fourth"


def test_advance_to_target_that_is_not_reachable_raises():
    model = TrackedModel()

    with pytest.raises(OrchestrationError, match="can not be reached"):
        model.advance_to("nope")


def test_advance_to_current_status_is_a_noop():
    model = TrackedModel()

    assert model.advance_to("first") == "first"
    assert model.moves == []


# ---------------------------------------------------------------------------
# fail_at - inject a failure at a specific stage
# ---------------------------------------------------------------------------
def test_fail_at_arms_failure_during_manual_progression():
    model = TrackedModel(failure_status="BROKEN", failure_reason_attr="reason")
    model.fail_at("second", reason="it broke")

    # The failure only happens when progression reaches the armed stage
    assert model.status == "first"

    model.advance()
    assert model.status == "BROKEN"
    assert model.is_frozen is True
    # The existing failure reason field of the service is reused
    assert model.reason == "it broke"
    failure = model.failure
    assert failure == {
        "stage": "second",
        "status": "BROKEN",
        "reason": "it broke",
        "trigger": "manual",
    }


def test_failure_freezes_automatic_progression():
    model = TrackedModel(failure_status="BROKEN")
    model.fail_at("third")

    with pytest.raises(OrchestrationError, match="failed at stage 'third'"):
        model.advance_to(None)

    assert model.status == "BROKEN"
    # Further describes do not move the resource anymore
    for _ in range(5):
        model.advance()
        _ = model.status
        assert model.status == "BROKEN"
    assert model.remaining_statuses == []


def test_fail_at_current_stage_applies_immediately():
    model = TrackedModel(failure_status="BROKEN")

    model.fail_at("first", reason="already broken")

    assert model.status == "BROKEN"
    assert model.is_frozen is True


def test_fail_at_without_failure_mapping_stays_on_the_stage():
    # Services without a dedicated failure status simply freeze on the stage
    model = TrackedModel()

    model.fail_at("second", reason="something went wrong")
    model.advance()

    assert model.status == "second"
    assert model.is_frozen is True
    assert model.failure["reason"] == "something went wrong"
    assert model.failure["status"] == "second"


def test_fail_at_explicit_failure_status_overrides_model_mapping():
    model = TrackedModel(failure_status="BROKEN")

    model.fail_at("second", failure_status="CUSTOM_BROKEN")
    model.advance()

    assert model.status == "CUSTOM_BROKEN"


def test_fail_at_invalid_stage_raises():
    model = TrackedModel()

    with pytest.raises(ValueError, match="not a valid stage"):
        model.fail_at("nope")


def test_fail_at_stage_on_a_separate_transition_chain():
    # Models like dsql::cluster have a create chain and a delete chain
    model = TrackedModel(
        transitions=[("CREATING", "ACTIVE"), ("DELETING", "DELETED")],
        failure_status={"DELETING": "DELETE_FAILED"},
    )
    # Arming is allowed for every declared stage, even on another chain
    model.fail_at("DELETING", reason="delete failed")
    model.advance_to("ACTIVE")
    assert model.status == "ACTIVE"
    assert model.is_frozen is False

    # The service starts the delete chain explicitly - the failure now fires
    model.status = "DELETING"
    model.advance()
    _ = model.status
    assert model.status == "DELETE_FAILED"
    assert model.is_frozen is True


def test_fail_at_with_immediate_progression_stops_at_armed_stage():
    state_manager.set_transition("example::tracked", {"progression": "immediate"})
    model = TrackedModel(failure_status="BROKEN")
    model.fail_at("third", reason="mid-flight")

    # Immediate progression would jump to 'fifth', but the failure stops it
    assert model.status == "BROKEN"
    assert model.failure["stage"] == "third"
    assert model.is_frozen is True


def test_clear_failure_allows_progression_again():
    model = TrackedModel()
    model.fail_at("second", reason="x")
    model.advance()
    _ = model.status  # progression happens when the status is read
    assert model.is_frozen is True

    model.clear_failure()
    # The resource was frozen on the stage itself (no failure mapping)
    assert model.is_frozen is False
    assert model.status == "second"
    model.advance_to(None)
    assert model.status == "fifth"


# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------
def test_observable_progression():
    model = TrackedModel()

    assert model.last_trigger is None
    assert model.remaining_statuses == ["second", "third", "fourth", "fifth"]

    model.advance()
    _ = model.status  # the transition is applied when the status is read
    assert model.last_trigger == "manual"
    assert model.remaining_statuses == ["third", "fourth", "fifth"]

    model.advance_to("fifth")
    state = model.orchestration_state()
    assert state == {
        "model_name": "example::tracked",
        "status": "fifth",
        "remaining": [],
        "last_trigger": "orchestration",
        "frozen": False,
        "failure": None,
    }


def test_remaining_statuses_on_cyclic_transitions_does_not_loop():
    cyclic = TrackedModel(
        model_name="example::cyclic",
        transitions=[("a", "b"), ("b", "c"), ("c", "a")],
    )

    assert cyclic.remaining_statuses == ["b", "c", "a"]


def test_last_trigger_for_time_progression():
    state_manager.set_transition(
        "example::tracked", {"progression": "time", "seconds": 0}
    )
    model = TrackedModel()

    # A zero-second transition is due immediately
    assert model.status == "second"
    assert model.last_trigger == "time"


# ---------------------------------------------------------------------------
# OrchestrationPlan - groups with dependencies
# ---------------------------------------------------------------------------
def test_plan_advances_in_dependency_order():
    first = TrackedModel()
    second = TrackedModel()
    third = TrackedModel()

    # Resources are added out of order - dependencies still determine the order
    plan = OrchestrationPlan()
    plan.add(third, target="fifth", depends_on=[second])
    plan.add(first, target="fifth")
    plan.add(second, target="fifth", depends_on=[first])

    results = plan.execute()

    assert [resource.status for resource in (first, second, third)] == [
        "fifth",
        "fifth",
        "fifth",
    ]
    assert results == {first: "fifth", second: "fifth", third: "fifth"}


def test_plan_rejects_adding_the_same_resource_twice():
    resource = TrackedModel()
    plan = OrchestrationPlan()
    plan.add(resource, target="fifth")

    with pytest.raises(ValueError, match="already added"):
        plan.add(resource, target="third")


def test_plan_chain_advances_in_given_order():
    resources = [TrackedModel() for _ in range(3)]

    OrchestrationPlan.chain(resources, target=["second", "fourth", "fifth"]).execute()

    assert [resource.status for resource in resources] == [
        "second",
        "fourth",
        "fifth",
    ]
    # Dependencies finish strictly before their dependents start
    assert resources[1].moves[0][0] == "first"
    assert resources[0].status == "second"


def test_plan_with_single_target_for_every_resource():
    resources = [TrackedModel() for _ in range(2)]

    OrchestrationPlan.chain(resources, target="third").execute()

    assert [resource.status for resource in resources] == ["third", "third"]


def test_plan_failure_blocks_dependents():
    first = TrackedModel(failure_status="BROKEN")
    first.fail_at("second")
    second = TrackedModel()

    plan = OrchestrationPlan()
    plan.add(first, target="fifth")
    plan.add(second, target="fifth", depends_on=[first])

    with pytest.raises(OrchestrationError, match="did not complete") as exc:
        plan.execute()

    # The failing resource is reported, the dependent is untouched
    assert exc.value.results[first] == "BROKEN"
    assert second.status == "first"


def test_plan_unknown_dependency_raises():
    resource = TrackedModel()
    other = TrackedModel()
    plan = OrchestrationPlan()
    plan.add(resource, target="fifth", depends_on=[other])

    with pytest.raises(OrchestrationError, match="not added to the plan"):
        plan.execute()


def test_plan_cycle_raises():
    first = TrackedModel()
    second = TrackedModel()
    plan = OrchestrationPlan()
    plan.add(first, target="third", depends_on=[second])
    plan.add(second, target="third", depends_on=[first])

    with pytest.raises(OrchestrationError, match="cyclic dependencies"):
        plan.execute()


def test_plan_rejects_non_managed_state_resources():
    plan = OrchestrationPlan()
    with pytest.raises(TypeError):
        plan.add("not-a-resource")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Concurrency - every step advances exactly once, status only moves forward
# ---------------------------------------------------------------------------
def test_concurrent_reads_advance_each_step_exactly_once():
    def run_trial() -> list[tuple]:
        model = TrackedModel()

        def worker() -> None:
            for _ in range(200):
                model.advance()
                _ = model.status

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return model.moves

    expected = [
        ("first", "second"),
        ("second", "third"),
        ("third", "fourth"),
        ("fourth", "fifth"),
    ]
    for _ in range(20):
        assert run_trial() == expected

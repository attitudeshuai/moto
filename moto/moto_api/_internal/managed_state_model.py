import threading
from datetime import datetime, timedelta
from typing import Any

from moto.moto_api import state_manager


class OrchestrationError(Exception):
    """
    Raised when an orchestrated progression cannot be completed, for example
    because the requested target status is not reachable or because a resource
    in an orchestrated group failed.
    """

    def __init__(self, message: str, results: dict[Any, str | None] | None = None):
        super().__init__(message)
        #: Statuses of the resources that were progressed before the failure
        self.results: dict[Any, str | None] = results or {}


class ManagedState:
    """
    Subclass this class to configure state-transitions.

    Three progression modes are supported out of the box (see
    :class:`moto.moto_api._internal.state_manager.StateManager`):

    * ``immediate`` - the status jumps to the final status as soon as it is read
    * ``manual``    - the status advances one step after the resource has been
                      described a configured number of times
    * ``time``      - the status advances one step after a configured delay

    On top of those modes the progression can be orchestrated per instance:

    * :meth:`advance_to` progresses the resource directly to a target status
    * :meth:`fail_at` injects a failure at a specific status. The resource
      freezes on that status and is no longer progressed automatically
    * :class:`moto.moto_api._internal.orchestration.OrchestrationPlan` advances
      a group of resources in an explicitly declared order

    Orchestration state is kept in memory on the instance itself - it is never
    persisted and does not replace the events/state-history of the individual
    services.
    """

    #: The status advanced because the configured progression is "immediate"
    TRIGGER_IMMEDIATE = "immediate"
    #: The status advanced because the resource was described (manual progression)
    TRIGGER_MANUAL = "manual"
    #: The status advanced because enough time has passed (time progression)
    TRIGGER_TIME = "time"
    #: The status advanced because an orchestration explicitly requested it
    TRIGGER_ORCHESTRATION = "orchestration"

    def __init__(
        self,
        model_name: str,
        transitions: list[tuple[str | None, str]],
        failure_status: str | dict[str, str] | None = None,
        failure_reason_attr: str | None = None,
    ):
        # Indicate the possible transitions for this model
        # Example: [(initializing,queued), (queued, starting), (starting, ready)]
        self._transitions = transitions
        # Current status of this model. Implementations should call `status`
        # The initial status is assumed to be the first transition
        self._status, _ = transitions[0]
        # Internal counter that keeps track of how often this model has been described
        # Used for transition-type=manual
        self._tick = 0
        # Time when the status was last progressed to this model
        # Used for transition-type=time
        self._time_progressed = datetime.now()
        # Name of this model. This will be used in the API
        self.model_name = model_name

        # How a failure at a given status is represented by this model.
        # Either a single failure status for every stage, or a mapping of
        # {normal_status: failure_status} (e.g. {"CREATING": "CREATE_FAILED"}).
        self._failure_status_config = failure_status
        # Attribute on the model that holds the human-readable failure reason.
        self._failure_reason_attr = failure_reason_attr

        # Orchestration state
        self._lock = threading.RLock()
        # What triggered the last progression (None until the first step)
        self._last_trigger: str | None = None
        # A failure that will be applied when progression reaches this stage
        self._armed_failure: dict[str, Any] | None = None
        # The failure that was actually applied, if any
        self._failure: dict[str, Any] | None = None
        # A frozen resource is no longer progressed automatically
        self._frozen = False

    def advance(self) -> None:
        """
        Signal that the resource was described.

        For ``manual`` progression this increments an internal counter; the
        actual transition happens the next time the status is read.
        """
        with self._lock:
            self._tick += 1

    @property
    def status(self) -> str | None:
        """
        Transitions the status as appropriate before returning
        """
        with self._lock:
            self._progress()
            return self._status

    @status.setter
    def status(self, value: str | None) -> None:
        # Explicit lifecycle changes by the services (e.g. starting a delete
        # chain) bypass the configured progression.
        with self._lock:
            self._status = value

    # Locks can not be pickled/copied - orchestration state is in-memory only
    # anyway, so a fresh lock is created on the copy.
    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()

    # ---------------------------------------------------------------------
    # Orchestration API
    # ---------------------------------------------------------------------
    def advance_to(
        self, target_status: str | None, trigger: str = TRIGGER_ORCHESTRATION
    ) -> str | None:
        """
        Progress the resource directly to ``target_status``, taking every
        transition in between in a single call, regardless of the configured
        progression mode.

        Pass ``None`` to force the resource all the way to its final status.

        Raises :class:`OrchestrationError` if the target status is not
        reachable or if a failure is injected while progressing.
        """
        with self._lock:
            if self._frozen:
                if target_status is None or self._status != target_status:
                    raise OrchestrationError(
                        f"'{self.model_name}' is frozen on failure at "
                        f"'{self._status}' and can not progress to "
                        f"'{target_status}'"
                    )
                return self._status
            if target_status is not None and self._status == target_status:
                return self._status
            visited: set[str | None] = set()
            while target_status is None or self._status != target_status:
                # The current status itself may be armed (e.g. after the service
                # started a different transition chain explicitly)
                if self._consume_armed_failure_at_current(trigger):
                    failure = self._failure or {}
                    raise OrchestrationError(
                        f"'{self.model_name}' failed at stage "
                        f"'{failure.get('stage')}'"
                        + (
                            f": {failure.get('reason')}"
                            if failure.get("reason")
                            else ""
                        )
                    )
                current = self._status
                if current in visited:
                    raise OrchestrationError(
                        f"Status '{target_status}' can not be reached for "
                        f"'{self.model_name}' (stuck at '{current}')"
                    )
                visited.add(current)
                nxt = self._get_next_status(current)
                if nxt == current:
                    if target_status is None or current == target_status:
                        break
                    raise OrchestrationError(
                        f"Status '{target_status}' can not be reached for "
                        f"'{self.model_name}' (stuck at '{current}')"
                    )
                self._take_step(trigger)
                if self._frozen:
                    failure = self._failure or {}
                    raise OrchestrationError(
                        f"'{self.model_name}' failed at stage "
                        f"'{failure.get('stage')}'"
                        + (
                            f": {failure.get('reason')}"
                            if failure.get("reason")
                            else ""
                        )
                    )
            return self._status

    def fail_at(
        self,
        stage: str,
        reason: str | None = None,
        failure_status: str | None = None,
        trigger: str = TRIGGER_ORCHESTRATION,
    ) -> None:
        """
        Inject a failure at the given ``stage``.

        When progression reaches that stage (through any trigger) the resource
        is moved to the failure status configured for this model, the reason is
        written to the model's failure-reason field (if any), and the resource
        freezes - it will no longer progress automatically.

        If the resource is already at ``stage`` the failure is applied
        immediately. ``failure_status`` overrides the failure status configured
        for the model.
        """
        with self._lock:
            if self._frozen:
                return
            valid_stages = {
                stage_name
                for transition in self._transitions
                for stage_name in transition
            }
            if stage != self._status and stage not in valid_stages:
                raise ValueError(
                    f"Status '{stage}' is not a valid stage for '{self.model_name}'"
                )
            self._armed_failure = {
                "stage": stage,
                "reason": reason,
                "status": failure_status,
            }
            if stage == self._status:
                self._consume_armed_failure_at_current(trigger)

    def clear_failure(self) -> None:
        """
        Remove an injected/armed failure and resume automatic progression.
        """
        with self._lock:
            self._armed_failure = None
            self._failure = None
            self._frozen = False

    @property
    def last_trigger(self) -> str | None:
        """
        What caused the last progression: ``immediate``, ``manual``, ``time``
        or ``orchestration``. ``None`` if the resource has never progressed.
        """
        return self._last_trigger

    @property
    def is_frozen(self) -> bool:
        """
        Whether a failure was injected and automatic progression has stopped.
        """
        return self._frozen

    @property
    def failure(self) -> dict[str, Any] | None:
        """
        Details of the applied failure: the stage it failed at, the failure
        status and the reason. ``None`` if no failure was applied.
        """
        return dict(self._failure) if self._failure else None

    @property
    def remaining_statuses(self) -> list[str | None]:
        """
        The statuses the resource can still progress to from its current
        status. An empty list when the resource is frozen or terminal.
        """
        with self._lock:
            if self._frozen:
                return []
            return self._statuses_from(self._status, include_current=False)

    def orchestration_state(self) -> dict[str, Any]:
        """
        Observable snapshot of the progression of this resource.
        """
        with self._lock:
            return {
                "model_name": self.model_name,
                "status": self._status,
                "remaining": self.remaining_statuses,
                "last_trigger": self._last_trigger,
                "frozen": self._frozen,
                "failure": self.failure,
            }

    # ---------------------------------------------------------------------
    # Internal progression logic
    # ---------------------------------------------------------------------
    def _progress(self) -> None:
        """
        Apply the configured progression mode. Every step goes through
        :meth:`_take_step`, so a step is taken at most once and progression is
        always monotonic, even when the status is read concurrently.
        """
        if self._frozen:
            return
        transition_config = state_manager.get_transition(self.model_name)
        progression = transition_config.get("progression")

        # A failure armed on the current status (possibly after the service
        # started another transition chain explicitly) fires before any step.
        trigger_by_progression = {
            "immediate": self.TRIGGER_IMMEDIATE,
            "manual": self.TRIGGER_MANUAL,
            "time": self.TRIGGER_TIME,
        }
        if progression in trigger_by_progression:
            trigger = trigger_by_progression[progression]
            if self._consume_armed_failure_at_current(trigger):
                return

        if progression == "immediate":
            while self._take_step(self.TRIGGER_IMMEDIATE):
                pass

        elif progression == "manual":
            if self._tick >= transition_config["times"]:
                self._take_step(self.TRIGGER_MANUAL)
                self._tick = 0

        elif progression == "time":
            next_transition_at = self._time_progressed + timedelta(
                seconds=transition_config["seconds"]
            )
            if datetime.now() > next_transition_at:
                self._take_step(self.TRIGGER_TIME)
                self._time_progressed = datetime.now()

    def _take_step(self, trigger: str) -> bool:
        """
        Move exactly one transition forward. Returns ``False`` when there is no
        transition from the current status. The caller must hold ``_lock``.
        """
        nxt = self._get_next_status(self._status)
        if nxt == self._status:
            return False
        self._land(nxt, trigger)
        return True

    def _consume_armed_failure_at_current(self, trigger: str) -> bool:
        """
        Apply a failure that is armed on the current status. Used when the
        service moved the resource onto an armed stage explicitly (e.g. by
        starting a delete chain). The caller must hold ``_lock``.
        """
        if self._armed_failure and self._armed_failure["stage"] == self._status:
            self._land(self._status, trigger)
            return True
        return False

    def _land(self, new_status: str | None, trigger: str) -> None:
        """
        Land on a new status, applying an armed failure if one is configured
        for that status. The caller must hold ``_lock``.
        """
        old_status = self._status
        applied_failure: dict[str, Any] | None = None
        if self._armed_failure and self._armed_failure["stage"] == new_status:
            armed = self._armed_failure
            self._armed_failure = None
            final_status = (
                armed["status"] or self._mapped_failure_status(new_status)
            )
            reason = armed["reason"]
            self._status = final_status
            self._frozen = True
            applied_failure = {
                "stage": new_status,
                "status": final_status,
                "reason": reason,
                "trigger": trigger,
            }
            self._failure = applied_failure
            if reason is not None and self._failure_reason_attr:
                setattr(self, self._failure_reason_attr, reason)
        else:
            self._status = new_status

        self._last_trigger = trigger
        self._on_status_change(old_status, self._status)
        if applied_failure:
            self._on_failure(applied_failure)

    def _mapped_failure_status(self, stage: str) -> str:
        """
        Resolve the failure status that belongs to a normal stage, reusing the
        failure statuses of the service. Falls back to the stage itself when
        the service has no specific failure status.
        """
        config = self._failure_status_config
        if isinstance(config, dict):
            return config.get(stage, stage)
        return config or stage

    def _on_status_change(self, old: str | None, new: str | None) -> None:
        """Hook called after every progression step. Override in subclasses."""

    def _on_failure(self, failure: dict[str, Any]) -> None:
        """Hook called after a failure is injected. Override in subclasses."""

    def _statuses_from(
        self, start: str | None, include_current: bool
    ) -> list[str | None]:
        """
        Follow the transitions from ``start``. Cycles do not cause an infinite
        loop - the walk stops when a status is encountered twice.
        """
        statuses: list[str | None] = []
        seen: set[str | None] = set()
        current = start
        if include_current:
            statuses.append(current)
            seen.add(current)
        while True:
            nxt = self._get_next_status(current)
            if nxt == current or nxt in seen:
                break
            statuses.append(nxt)
            seen.add(nxt)
            current = nxt
        return statuses

    def _get_next_status(self, previous: str | None) -> str | None:
        return next(
            (nxt for prev, nxt in self._transitions if previous == prev), previous
        )

    def _get_last_status(self, previous: str | None) -> str | None:
        next_state = self._get_next_status(previous)
        while next_state != previous:
            previous = next_state
            next_state = self._get_next_status(previous)
        return next_state

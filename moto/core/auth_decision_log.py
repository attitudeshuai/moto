"""
Observation surface and failure injection for the simulated IAM authorization.

Nothing in this module is active unless IAM authentication is enabled (see
``enable_iam_authentication`` / ``INITIAL_NO_AUTH_ACTION_COUNT``). While
authentication is enabled, every authorization decision is appended to a
bounded, thread-safe in-memory log and configured injection rules can force a
denial for specific actions/resources.

An injected denial:

* is raised through the exact same code path as a real policy denial (same
  exception, same response body), so callers cannot tell the difference;
* is always marked on the recorded decision (``injected=True`` /
  ``source="injection"``), so it can never be mistaken for a policy verdict.
"""

from __future__ import annotations

import fnmatch
import threading
import uuid
from collections import deque
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests

from moto import settings


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DecisionEffect:
    ALLOW = "allow"
    DENY = "deny"


class DecisionSource:
    """Where a decision came from."""

    SIGNATURE = "signature"  # access key / signature verification
    POLICY = "policy"  # IAM (trust) policy evaluation
    INJECTION = "injection"  # forced by a configured injection rule


class DenyCategory:
    """The reason a decision was denied."""

    SIGNATURE_MISMATCH = "signature_mismatch"
    INVALID_ACCESS_KEY = "invalid_access_key"
    EXPLICIT_DENY = "explicit_deny"
    IMPLICIT_DENY = "implicit_deny"
    TRUST_POLICY_DENY = "trust_policy_deny"
    INJECTED = "injected"


def _as_pattern_tuple(value: str | Sequence[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(value)


def _pattern_matches(pattern: str, value: str) -> bool:
    # Match against the full "service:Action" name and against the bare action
    # suffix, so both "sqs:SendMessage" and "SendMessage" work as selectors.
    if fnmatch.fnmatchcase(value, pattern):
        return True
    if ":" in value and fnmatch.fnmatchcase(value.split(":", 1)[1], pattern):
        return True
    return False


@dataclass(frozen=True)
class InjectionRule:
    """A forced-deny rule.

    A request matches when its action matches one of ``actions`` (fnmatch
    patterns against ``service:Action``) AND its resource matches one of
    ``resources`` (fnmatch patterns against the resource ARN). An empty tuple
    means that side of the rule matches everything.
    """

    name: str
    actions: tuple[str, ...] = ()
    resources: tuple[str, ...] = ()

    def matches(self, action: str, resource: str) -> bool:
        action_matches = not self.actions or any(
            _pattern_matches(pattern, action) for pattern in self.actions
        )
        resource_matches = not self.resources or any(
            fnmatch.fnmatchcase(resource, pattern) for pattern in self.resources
        )
        return action_matches and resource_matches

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "actions": list(self.actions),
            "resources": list(self.resources),
        }


@dataclass(frozen=True)
class StatementDecision:
    """What a single policy statement contributed to the decision."""

    index: int
    effect: str | None  # "Allow" / "Deny" / None when the statement did not apply
    action_matched: bool
    # None when the statement has no Resource element (trust policies)
    resource_matched: bool | None
    matched_resource_pattern: str | None
    # None when the statement did not check the condition/principal
    conditions_matched: bool | None
    principal_matched: bool | None
    result: str  # name of a PermissionResult: PERMITTED / DENIED / NEUTRAL

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "effect": self.effect,
            "action_matched": self.action_matched,
            "resource_matched": self.resource_matched,
            "matched_resource_pattern": self.matched_resource_pattern,
            "conditions_matched": self.conditions_matched,
            "principal_matched": self.principal_matched,
            "result": self.result,
        }


@dataclass(frozen=True)
class PolicyDecision:
    """The outcome of evaluating one (identity or trust) policy."""

    policy_id: str | None  # policy ARN / inline policy name when known
    kind: str  # "identity" or "trust"
    statements: tuple[StatementDecision, ...]
    result: str  # name of a PermissionResult

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "kind": self.kind,
            "statements": [statement.to_dict() for statement in self.statements],
            "result": self.result,
        }


@dataclass
class AuthorizationEvaluation:
    """Mutable, request-local working state for one authorization decision.

    A new instance is created per request, so concurrent requests can never
    share or mix evaluation state.
    """

    action: str
    resource: str
    principal: str | None = None
    effect: str | None = None
    deny_category: str | None = None
    deny_reason: str | None = None
    notes: str | None = None
    injection_rule: InjectionRule | None = None
    policy_decisions: list[PolicyDecision] = field(default_factory=list)
    trust_policy_decisions: list[PolicyDecision] = field(default_factory=list)

    @property
    def denied(self) -> bool:
        return self.effect == DecisionEffect.DENY

    def add_policy_decision(
        self, decision: PolicyDecision, *, trust: bool = False
    ) -> None:
        if trust:
            self.trust_policy_decisions.append(decision)
        else:
            self.policy_decisions.append(decision)

    def allow(self, notes: str | None = None) -> None:
        self.effect = DecisionEffect.ALLOW
        if notes is not None:
            self.notes = notes

    def deny(
        self,
        category: str,
        reason: str | None = None,
        injection_rule: InjectionRule | None = None,
    ) -> None:
        self.effect = DecisionEffect.DENY
        self.deny_category = category
        self.deny_reason = reason
        if injection_rule is not None:
            self.injection_rule = injection_rule


@dataclass(frozen=True)
class AuthDecisionRecord:
    """An immutable record of one authorization decision."""

    sequence: int
    request_id: str
    timestamp: datetime
    account_id: str
    region: str | None
    service: str | None
    action: str
    resource: str
    principal: str | None
    effect: str
    source: str
    deny_category: str | None
    deny_reason: str | None
    error_code: str | None
    injected: bool
    policy_decisions: tuple[PolicyDecision, ...]
    trust_policy_decisions: tuple[PolicyDecision, ...]
    injection_rule: InjectionRule | None
    notes: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "request_id": self.request_id,
            "timestamp": self.timestamp.isoformat(),
            "account_id": self.account_id,
            "region": self.region,
            "service": self.service,
            "action": self.action,
            "resource": self.resource,
            "principal": self.principal,
            "effect": self.effect,
            "source": self.source,
            "deny_category": self.deny_category,
            "deny_reason": self.deny_reason,
            "error_code": self.error_code,
            "injected": self.injected,
            "policy_decisions": [
                decision.to_dict() for decision in self.policy_decisions
            ],
            "trust_policy_decisions": [
                decision.to_dict() for decision in self.trust_policy_decisions
            ],
            "injection_rule": (
                self.injection_rule.to_dict() if self.injection_rule else None
            ),
            "notes": self.notes,
        }


class AuthDecisionLog:
    """Bounded, thread-safe log of authorization decisions plus injection rules."""

    DEFAULT_MAX_RECORDS = 1000

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._records: deque[AuthDecisionRecord] = deque()
        self._injection_rules: dict[str, InjectionRule] = {}
        self._max_records: int = self.DEFAULT_MAX_RECORDS
        self._dropped_records: int = 0
        self._sequence: int = 0
        self._rule_sequence: int = 0

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------
    def configure(self, max_records: int | None = None) -> None:
        """Set the maximum number of records kept.

        When the cap is reached, the oldest records are discarded and the
        dropped counter is incremented.
        """
        if max_records is not None:
            if not isinstance(max_records, int) or max_records < 0:
                raise ValueError("max_records must be a non-negative integer")
            with self._lock:
                self._max_records = max_records
                while len(self._records) > self._max_records:
                    self._records.popleft()
                    self._dropped_records += 1

    def reset(self) -> None:
        """Drop all records and zero the dropped counter. Rules are kept."""
        with self._lock:
            self._records.clear()
            self._dropped_records = 0
            self._sequence = 0

    def reset_all(self) -> None:
        """Drop every record and every injection rule."""
        with self._lock:
            self.reset()
            self._injection_rules.clear()
            self._rule_sequence = 0

    @property
    def capacity(self) -> int:
        with self._lock:
            return self._max_records

    @property
    def dropped_records(self) -> int:
        with self._lock:
            return self._dropped_records

    # ------------------------------------------------------------------
    # Failure injection
    # ------------------------------------------------------------------
    def add_injection_rule(
        self,
        actions: str | Sequence[str] | None = None,
        resources: str | Sequence[str] | None = None,
        name: str | None = None,
    ) -> str:
        if actions is None and resources is None:
            raise ValueError(
                "An injection rule must target at least an action or a resource"
            )
        with self._lock:
            if name is None:
                self._rule_sequence += 1
                name = f"injection-{self._rule_sequence}"
            elif name in self._injection_rules:
                raise ValueError(f"Injection rule '{name}' already exists")
            rule = InjectionRule(
                name=name,
                actions=_as_pattern_tuple(actions),
                resources=_as_pattern_tuple(resources),
            )
            self._injection_rules[name] = rule
            return name

    def remove_injection_rule(self, name: str) -> None:
        with self._lock:
            self._injection_rules.pop(name, None)

    def clear_injection_rules(self) -> None:
        with self._lock:
            self._injection_rules.clear()

    def injection_rules(self) -> tuple[InjectionRule, ...]:
        with self._lock:
            return tuple(self._injection_rules.values())

    def find_injection_rule(self, action: str, resource: str) -> InjectionRule | None:
        with self._lock:
            for rule in self._injection_rules.values():
                if rule.matches(action, resource):
                    return rule
        return None

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------
    def record(
        self,
        *,
        request_id: str,
        account_id: str,
        region: str | None,
        service: str | None,
        evaluation: AuthorizationEvaluation,
        error_code: str | None = None,
    ) -> AuthDecisionRecord:
        if evaluation.injection_rule is not None:
            source = DecisionSource.INJECTION
        elif evaluation.deny_category in (
            DenyCategory.SIGNATURE_MISMATCH,
            DenyCategory.INVALID_ACCESS_KEY,
        ):
            source = DecisionSource.SIGNATURE
        else:
            source = DecisionSource.POLICY

        with self._lock:
            self._sequence += 1
            record = AuthDecisionRecord(
                sequence=self._sequence,
                request_id=request_id,
                timestamp=_utcnow(),
                account_id=account_id,
                region=region,
                service=service,
                action=evaluation.action,
                resource=evaluation.resource,
                principal=evaluation.principal,
                effect=evaluation.effect or DecisionEffect.DENY,
                source=source,
                deny_category=evaluation.deny_category,
                deny_reason=evaluation.deny_reason,
                error_code=error_code,
                injected=source == DecisionSource.INJECTION,
                policy_decisions=tuple(evaluation.policy_decisions),
                trust_policy_decisions=tuple(evaluation.trust_policy_decisions),
                injection_rule=evaluation.injection_rule,
                notes=evaluation.notes,
            )
            if self._max_records <= 0:
                self._dropped_records += 1
                return record
            while len(self._records) >= self._max_records:
                self._records.popleft()
                self._dropped_records += 1
            self._records.append(record)
        return record

    # ------------------------------------------------------------------
    # Observation
    # ------------------------------------------------------------------
    def decisions(
        self,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        request_id: str | None = None,
        action: str | None = None,
        resource: str | None = None,
        effect: str | None = None,
        denied_only: bool = False,
        injected_only: bool | None = None,
    ) -> tuple[AuthDecisionRecord, ...]:
        """Return an ordered snapshot of records matching the filters.

        ``since`` is inclusive and ``until`` exclusive. ``action`` /
        ``resource`` are fnmatch patterns.
        """
        with self._lock:
            records = tuple(self._records)
        filtered: list[AuthDecisionRecord] = []
        for record in records:
            if since is not None and record.timestamp < since:
                continue
            if until is not None and record.timestamp >= until:
                continue
            if request_id is not None and record.request_id != request_id:
                continue
            if action is not None and not _pattern_matches(action, record.action):
                continue
            if resource is not None and not fnmatch.fnmatchcase(
                record.resource, resource
            ):
                continue
            if effect is not None and record.effect != effect:
                continue
            if denied_only and record.effect != DecisionEffect.DENY:
                continue
            if injected_only is not None and record.injected != injected_only:
                continue
            filtered.append(record)
        return tuple(filtered)


# ----------------------------------------------------------------------
# Process-wide singleton
# ----------------------------------------------------------------------
_auth_decision_log: AuthDecisionLog | None = None
_auth_decision_log_lock = threading.Lock()


def get_auth_decision_log() -> AuthDecisionLog:
    """Return the process-wide AuthDecisionLog singleton."""
    global _auth_decision_log
    if _auth_decision_log is None:
        with _auth_decision_log_lock:
            if _auth_decision_log is None:
                _auth_decision_log = AuthDecisionLog()
    return _auth_decision_log


# ----------------------------------------------------------------------
# Public convenience API
# ----------------------------------------------------------------------
def _server_mode_endpoint() -> str:
    return settings.test_server_mode_endpoint()


def configure_auth_decisions(max_records: int) -> None:
    """Configure the decision log (currently: the record capacity)."""
    if settings.TEST_SERVER_MODE:
        requests.post(
            f"{_server_mode_endpoint()}/moto-api/auth/decisions/configure",
            json={"max_records": max_records},
            timeout=60,
        )
    else:
        get_auth_decision_log().configure(max_records=max_records)


def reset_auth_decisions() -> None:
    """Clear all recorded decisions (and injection rules)."""
    if settings.TEST_SERVER_MODE:
        requests.post(
            f"{_server_mode_endpoint()}/moto-api/auth/decisions/reset", timeout=60
        )
    else:
        get_auth_decision_log().reset_all()


def dropped_auth_decisions() -> int:
    """Number of records discarded because the capacity was reached."""
    if settings.TEST_SERVER_MODE:
        response = requests.get(
            f"{_server_mode_endpoint()}/moto-api/auth/decisions", timeout=60
        )
        return int(response.json()["dropped_records"])
    return get_auth_decision_log().dropped_records


def get_auth_decisions(
    *,
    since: datetime | str | None = None,
    until: datetime | str | None = None,
    request_id: str | None = None,
    action: str | None = None,
    resource: str | None = None,
    denied_only: bool = False,
    injected_only: bool | None = None,
) -> tuple[AuthDecisionRecord, ...] | tuple[dict[str, Any], ...]:
    """Return recorded decisions (newest-last), optionally filtered.

    In decorator mode immutable :class:`AuthDecisionRecord` objects are
    returned; in server mode the equivalent plain dicts are returned.
    """
    if settings.TEST_SERVER_MODE:
        params: dict[str, str] = {}
        if request_id is not None:
            params["request_id"] = request_id
        if action is not None:
            params["action"] = action
        if resource is not None:
            params["resource"] = resource
        if denied_only:
            params["denied_only"] = "true"
        if injected_only is not None:
            params["injected_only"] = "true" if injected_only else "false"
        if since is not None:
            params["since"] = (
                since.isoformat() if isinstance(since, datetime) else since
            )
        if until is not None:
            params["until"] = (
                until.isoformat() if isinstance(until, datetime) else until
            )
        response = requests.get(
            f"{_server_mode_endpoint()}/moto-api/auth/decisions",
            params=params,
            timeout=60,
        )
        return tuple(response.json()["decisions"])

    if isinstance(since, str):
        since = datetime.fromisoformat(since)
    if isinstance(until, str):
        until = datetime.fromisoformat(until)
    return get_auth_decision_log().decisions(
        since=since,
        until=until,
        request_id=request_id,
        action=action,
        resource=resource,
        denied_only=denied_only,
        injected_only=injected_only,
    )


def add_auth_failure_injection(
    actions: str | Sequence[str] | None = None,
    resources: str | Sequence[str] | None = None,
    name: str | None = None,
) -> str:
    """Force every matching request to be denied while IAM auth is enabled.

    The denial reuses the normal policy-denial error path, but recorded
    decisions are marked as injected.
    """
    if settings.TEST_SERVER_MODE:
        response = requests.post(
            f"{_server_mode_endpoint()}/moto-api/auth/injections",
            json={"name": name, "actions": actions, "resources": resources},
            timeout=60,
        )
        return str(response.json()["name"])
    return get_auth_decision_log().add_injection_rule(
        actions=actions, resources=resources, name=name
    )


def remove_auth_failure_injection(name: str) -> None:
    """Remove an injection rule. Matching requests use real decisions again."""
    if settings.TEST_SERVER_MODE:
        requests.post(
            f"{_server_mode_endpoint()}/moto-api/auth/injections/remove",
            json={"name": name},
            timeout=60,
        )
    else:
        get_auth_decision_log().remove_injection_rule(name)


def clear_auth_failure_injections() -> None:
    """Remove all injection rules."""
    if settings.TEST_SERVER_MODE:
        requests.post(
            f"{_server_mode_endpoint()}/moto-api/auth/injections/clear", timeout=60
        )
    else:
        get_auth_decision_log().clear_injection_rules()


@contextmanager
def inject_auth_failure(
    actions: str | Sequence[str] | None = None,
    resources: str | Sequence[str] | None = None,
    name: str | None = None,
) -> Iterator[str]:
    """Context manager adding an injection rule and removing it on exit."""
    rule_name = add_auth_failure_injection(
        actions=actions, resources=resources, name=name
    )
    try:
        yield rule_name
    finally:
        remove_auth_failure_injection(rule_name)


def new_request_id() -> str:
    return uuid.uuid4().hex

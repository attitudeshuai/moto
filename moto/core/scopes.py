"""
Caller-scoped resource views for the Moto server.

Without a scope header Moto keeps one process-wide resource view: every caller
shares the same backends and a single ``/moto-api/reset`` wipes everything.

This module adds an optional isolation boundary on top of the existing
account/region dimensions:

- A caller identifies itself by sending the ``x-moto-scope-id`` header on every
  request. Requests with the same scope id share one resource view; different
  scope ids are fully isolated (identical resource names can coexist and
  list/describe calls only return resources owned by the current scope).
- Scopes are created/destroyed through the ``/moto-api/scopes`` API. A request
  that references an unknown or already-released scope fails with an explicit
  error instead of silently falling back to another view.
- The scope binding is established when a request enters the dispatch layer and
  released when the request finishes, so worker threads can never leak a scope
  identity to a subsequent request.
- Destroying a scope waits for in-flight requests to finish and then removes
  that scope's data only. Other scopes keep working and can be created or
  destroyed concurrently. No request can observe a half-reset scope.
- Requests that do not carry the scope header use the default (``None``)
  partition, which behaves exactly like the old process-wide state.

The active scope is propagated through a :class:`contextvars.ContextVar`, which
works for both the threaded Werkzeug server and in-process (decorator) usage.
"""

from __future__ import annotations

import contextvars
import json
import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

#: Header used by callers to declare which scope their request belongs to.
SCOPE_HEADER = "x-moto-scope-id"

#: Scope ids are caller-provided identifiers. Restrict the character set so the
#: id can safely travel through headers and URL paths (colons are allowed for
#: ARN-like identifiers).
_SCOPE_ID_REGEX = re.compile(r"^[A-Za-z0-9._:\-]{1,128}$")


class ScopeError(Exception):
    """Base class for scope related request errors."""

    status_code = 400
    error_code = "InvalidScope"

    def __init__(self, scope_id: str | None, message: str):
        super().__init__(message)
        self.scope_id = scope_id
        self.message = message

    def to_response(self) -> tuple[int, dict[str, str], str]:
        headers = {"Content-Type": "application/json"}
        body = json.dumps(
            {"__type": self.error_code, "message": self.message}
            | ({"ScopeId": self.scope_id} if self.scope_id is not None else {})
        )
        return self.status_code, headers, body


class ScopeNotFoundError(ScopeError):
    """Raised when a request references a scope that does not exist (anymore)."""

    status_code = 404
    error_code = "ScopeNotFound"

    def __init__(self, scope_id: str):
        super().__init__(
            scope_id,
            f"Scope '{scope_id}' does not exist or has already been released.",
        )


class InvalidScopeIdError(ScopeError):
    """Raised when a request carries a malformed scope identifier."""

    status_code = 400
    error_code = "InvalidScopeId"

    def __init__(self, scope_id: str):
        super().__init__(
            scope_id,
            "Invalid scope id: scope ids must be 1-128 characters long and may "
            "only contain letters, digits, dots, underscores, colons and "
            f"hyphens. Got: '{scope_id}'.",
        )


@dataclass
class Scope:
    """A single caller scope.

    ``uid`` is a unique, random partition key: recreating a scope with the same
    caller-provided id after a close() always yields a fresh partition, so a
    late teardown can never wipe data of a recreated scope.
    """

    id: str
    uid: str
    active_requests: int = 0
    closing: bool = False


class ScopeRegistry:
    """Thread-safe registry of live scopes."""

    def __init__(self) -> None:
        # One condition variable guards the scope map and coordinates the
        # "wait for in-flight requests" step of close().
        self._cond = threading.Condition(threading.Lock())
        self._scopes: dict[str, Scope] = {}

    def create(self, scope_id: str) -> bool:
        """Create a scope. Returns True if created, False if it already existed."""
        if not _SCOPE_ID_REGEX.match(scope_id):
            raise InvalidScopeIdError(scope_id)
        with self._cond:
            if scope_id in self._scopes:
                return False
            self._scopes[scope_id] = Scope(id=scope_id, uid=uuid4().hex)
            return True

    def exists(self, scope_id: str) -> bool:
        with self._cond:
            scope = self._scopes.get(scope_id)
            return scope is not None and not scope.closing

    def list_scope_ids(self) -> list[str]:
        with self._cond:
            return sorted(
                scope_id
                for scope_id, scope in self._scopes.items()
                if not scope.closing
            )

    def acquire(self, scope_id: str) -> Scope:
        """Bind an incoming request to its scope.

        Raises ScopeNotFoundError for unknown scopes or scopes that are being
        closed, so requests can never land in a different/empty view.
        """
        with self._cond:
            scope = self._scopes.get(scope_id)
            if scope is None or scope.closing:
                raise ScopeNotFoundError(scope_id)
            scope.active_requests += 1
            return scope

    def release(self, scope: Scope) -> None:
        """Release a request binding, waking up a concurrent close() if needed."""
        with self._cond:
            scope.active_requests -= 1
            if scope.active_requests <= 0:
                if scope.active_requests < 0:  # pragma: no cover - defensive
                    raise RuntimeError(
                        f"Scope '{scope.id}' was released more times than acquired"
                    )
                self._cond.notify_all()

    def close(self, scope_id: str) -> bool:
        """Release a scope and all of its resources.

        The scope is first marked as closing and removed from the registry so
        no new request can enter it. close() then waits for in-flight requests
        of THIS scope to finish before tearing the data down. Other scopes are
        untouched and remain fully available while the teardown runs.
        """
        with self._cond:
            scope = self._scopes.get(scope_id)
            if scope is None:
                return False
            scope.closing = True
            # Condition.wait_for releases the lock while waiting, so creating
            # and closing other scopes can proceed concurrently.
            self._cond.wait_for(lambda: scope.active_requests == 0)
            del self._scopes[scope_id]
            uid = scope.uid

        # Tear the partition down outside the registry lock. The uid belongs to
        # this scope incarnation only, so a scope recreated with the same id is
        # not affected. No request can reach the old partition anymore.
        from moto.core.base_backend import BackendDict

        BackendDict.reset_partition(uid)
        return True

    def reset(self) -> None:
        """Remove every scope and its data (used by the global reset API)."""
        with self._cond:
            uids = [scope.uid for scope in self._scopes.values()]
            self._scopes.clear()

        from moto.core.base_backend import BackendDict

        for uid in uids:
            BackendDict.reset_partition(uid)

    def __len__(self) -> int:
        with self._cond:
            return sum(1 for scope in self._scopes.values() if not scope.closing)

    def __iter__(self) -> Iterator[str]:
        return iter(self.list_scope_ids())


#: Process-wide registry. Scopes only exist when explicitly created through the
#: server management API, so in decorator mode this stays empty.
scope_registry = ScopeRegistry()

#: The scope bound to the current request. ``None`` means the request does not
#: use scopes and must see the legacy process-wide state.
_current_scope: contextvars.ContextVar[Scope | None] = contextvars.ContextVar(
    "moto_current_scope", default=None
)


def get_scope_id(headers: Any) -> str | None:
    """Extract a scope id from a request headers mapping, case-insensitively."""
    if headers is None:
        return None
    value = headers.get(SCOPE_HEADER)
    if value is None:
        # Werkzeug EnvironHeaders are case-insensitive, but headers can also
        # arrive as a plain dict (e.g. dict(request.headers)) with title-cased
        # keys, so fall back to a case-insensitive scan.
        try:
            items = headers.items()
        except AttributeError:
            return None
        for key, candidate in items:
            if key.lower() == SCOPE_HEADER:
                value = candidate
                break
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    value = value.strip()
    return value or None


def current_scope() -> Scope | None:
    return _current_scope.get()


def current_partition_uid() -> str | None:
    """Partition key BackendDict uses to locate the current request's data."""
    scope = _current_scope.get()
    return None if scope is None else scope.uid


@contextmanager
def scope_for_request(scope_id: str | None) -> Iterator[None]:
    """Bind a request to its scope for the duration of the wrapped block.

    No/empty scope id -> the legacy default view is used and nothing is
    tracked. An invalid id raises InvalidScopeIdError; an unknown/released id
    raises ScopeNotFoundError before any backend is touched.
    """
    if scope_id is None:
        yield
        return
    if not _SCOPE_ID_REGEX.match(scope_id):
        raise InvalidScopeIdError(scope_id)
    scope = scope_registry.acquire(scope_id)
    token = _current_scope.set(scope)
    try:
        yield
    finally:
        _current_scope.reset(token)
        scope_registry.release(scope)


def bind_request(scope_id: str | None) -> tuple[Scope, contextvars.Token[Scope | None]] | None:
    """Acquire a scope and bind it to the current request thread/task."""
    if scope_id is None:
        return None
    if not _SCOPE_ID_REGEX.match(scope_id):
        raise InvalidScopeIdError(scope_id)
    scope = scope_registry.acquire(scope_id)
    token = _current_scope.set(scope)
    return scope, token


def unbind_request(
    binding: tuple[Scope, contextvars.Token[Scope | None]] | None,
) -> None:
    """Reverse :func:`bind_request`; safe to call with ``None``."""
    if binding is None:
        return
    scope, token = binding
    _current_scope.reset(token)
    scope_registry.release(scope)


def run_with_scope(headers: Any, func: Any, *args: Any, **kwargs: Any) -> Any:
    """Invoke ``func`` with the caller scope declared in ``headers``.

    Single chokepoint used by both transports (the in-process Botocore
    stubber and the Flask server wrapper), so every service response class -
    including services with custom dispatch entry points such as S3 - is
    covered. An unknown/invalid scope produces the explicit scope error
    response tuple instead of running the request.
    """
    try:
        binding = bind_request(get_scope_id(headers))
    except ScopeError as err:
        return err.to_response()
    try:
        return func(*args, **kwargs)
    finally:
        unbind_request(binding)


# Re-exported for typing convenience.
__all__ = [
    "SCOPE_HEADER",
    "Scope",
    "ScopeError",
    "ScopeNotFoundError",
    "InvalidScopeIdError",
    "ScopeRegistry",
    "scope_registry",
    "bind_request",
    "current_partition_uid",
    "current_scope",
    "get_scope_id",
    "run_with_scope",
    "scope_for_request",
    "unbind_request",
]

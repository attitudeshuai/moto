import threading
from threading import Event, Thread

import pytest

from moto.core.base_backend import BackendDict, BaseBackend
from moto.core.scopes import (
    InvalidScopeIdError,
    ScopeNotFoundError,
    ScopeRegistry,
    bind_request,
    current_partition_uid,
    current_scope,
    get_scope_id,
    scope_for_request,
    scope_registry,
    unbind_request,
)


class ExampleBackend(BaseBackend):
    def __init__(self, region_name: str, account_id: str):
        super().__init__(region_name, account_id)
        self.items: list[str] = []


@pytest.fixture(autouse=True)
def reset_scopes_and_backends() -> None:
    scope_registry.reset()
    BackendDict.reset()
    yield
    scope_registry.reset()
    BackendDict.reset()


# ---------------------------------------------------------------------------
# ScopeRegistry lifecycle
# ---------------------------------------------------------------------------


def test_create_is_idempotent() -> None:
    assert scope_registry.create("tenant-1") is True
    assert scope_registry.create("tenant-1") is False
    assert scope_registry.list_scope_ids() == ["tenant-1"]


def test_create_rejects_invalid_ids() -> None:
    for bad_id in ["", "with space", "slash/x", "x" * 129, "dollar$"]:
        with pytest.raises(InvalidScopeIdError):
            scope_registry.create(bad_id)


def test_acquire_unknown_scope_fails_explicitly() -> None:
    with pytest.raises(ScopeNotFoundError):
        scope_registry.acquire("never-created")


def test_closed_scope_can_no_longer_be_acquired() -> None:
    scope_registry.create("tenant-1")
    assert scope_registry.close("tenant-1") is True
    with pytest.raises(ScopeNotFoundError):
        scope_registry.acquire("tenant-1")
    # Closing it again is also explicit
    assert scope_registry.close("tenant-1") is False


def test_recreated_scope_gets_fresh_partition() -> None:
    scope_registry.create("tenant-1")
    first = scope_registry.acquire("tenant-1")
    scope_registry.release(first)
    scope_registry.close("tenant-1")

    scope_registry.create("tenant-1")
    second = scope_registry.acquire("tenant-1")
    scope_registry.release(second)
    try:
        assert second.uid != first.uid
    finally:
        scope_registry.close("tenant-1")


# ---------------------------------------------------------------------------
# Request binding
# ---------------------------------------------------------------------------


def test_bind_without_scope_id_uses_legacy_view() -> None:
    assert bind_request(None) is None
    assert current_scope() is None
    assert current_partition_uid() is None
    unbind_request(None)  # must be a no-op


def test_bind_resolves_scope_and_unbind_restores_previous() -> None:
    scope_registry.create("tenant-1")
    binding = bind_request("tenant-1")
    assert binding is not None
    assert current_scope() is not None
    assert current_partition_uid() == binding[0].uid
    unbind_request(binding)
    assert current_scope() is None


def test_bind_unknown_or_invalid_scope() -> None:
    with pytest.raises(ScopeNotFoundError):
        bind_request("ghost")
    with pytest.raises(InvalidScopeIdError):
        bind_request("bad id")


def test_binding_does_not_leak_to_other_threads() -> None:
    scope_registry.create("tenant-1")
    seen: dict[str, object] = {}
    thread_ready = Event()
    main_release = Event()

    def worker() -> None:
        thread_ready.set()
        main_release.wait(2)
        seen["scope"] = current_scope()

    t = Thread(target=worker)
    t.start()
    thread_ready.wait(2)
    binding = bind_request("tenant-1")
    try:
        main_release.set()
        t.join()
    finally:
        unbind_request(binding)
    assert seen["scope"] is None


def test_scope_context_manager_binds_and_releases() -> None:
    scope_registry.create("tenant-1")
    with scope_for_request("tenant-1"):
        assert current_scope() is not None
        assert current_partition_uid() is not None
    assert current_scope() is None

    # No scope id -> legacy view, no tracking
    with scope_for_request(None):
        assert current_scope() is None

    with pytest.raises(ScopeNotFoundError):
        with scope_for_request("ghost"):
            pass
    assert current_scope() is None


def test_nested_bindings_are_released_in_order() -> None:
    scope_registry.create("tenant-1")
    outer = bind_request("tenant-1")
    inner = bind_request("tenant-1")
    try:
        scope = current_scope()
        assert scope is not None
        assert scope.active_requests == 2
    finally:
        unbind_request(inner)
        assert current_scope() is not None
        unbind_request(outer)
    assert current_scope() is None


def test_get_scope_id_is_case_insensitive() -> None:
    assert get_scope_id({"X-Moto-Scope-Id": "abc"}) == "abc"
    assert get_scope_id({"x-moto-scope-id": "abc"}) == "abc"
    assert get_scope_id({"other": "x"}) is None
    assert get_scope_id({"x-moto-scope-id": "   "}) is None
    assert get_scope_id(None) is None


# ---------------------------------------------------------------------------
# BackendDict partition isolation
# ---------------------------------------------------------------------------


def test_default_partition_matches_legacy_behaviour() -> None:
    backend_dict = BackendDict(ExampleBackend, "ec2")
    assert list(backend_dict.items()) == []

    backend = backend_dict["123456789012"]["us-east-1"]
    assert isinstance(backend, ExampleBackend)
    assert list(backend_dict.keys()) == ["123456789012"]
    assert "123456789012" in backend_dict
    assert len(backend_dict) == 1


def test_scoped_partitions_are_independent() -> None:
    backend_dict = BackendDict(ExampleBackend, "ec2")
    default_backend = backend_dict["123456789012"]["us-east-1"]
    default_backend.items.append("default-resource")

    scope_registry.create("tenant-1")
    scope_registry.create("tenant-2")

    binding_1 = bind_request("tenant-1")
    try:
        # The account/region does not exist in this scope yet
        assert "123456789012" not in backend_dict
        scope_backend_1 = backend_dict["123456789012"]["us-east-1"]
        assert scope_backend_1 is not default_backend
        scope_backend_1.items.append("a-resource")

        assert [
            region
            for _, region, _ in backend_dict.iter_backends()
            if region == "us-east-1"
        ] == ["us-east-1"]
        assert backend_dict["123456789012"]["us-east-1"].items == ["a-resource"]
    finally:
        unbind_request(binding_1)

    binding_2 = bind_request("tenant-2")
    try:
        scope_backend_2 = backend_dict["123456789012"]["us-east-1"]
        assert scope_backend_2 is not default_backend
        # Same resource name can exist independently; nothing leaks across
        assert scope_backend_2.items == []
        scope_backend_2.items.append("b-resource")
        assert backend_dict["123456789012"]["us-east-1"].items == ["b-resource"]
    finally:
        unbind_request(binding_2)

    binding_1 = bind_request("tenant-1")
    try:
        assert backend_dict["123456789012"]["us-east-1"].items == ["a-resource"]
    finally:
        unbind_request(binding_1)

    # The legacy view never saw any of the scoped data
    assert default_backend.items == ["default-resource"]
    assert backend_dict["123456789012"]["us-east-1"] is default_backend


def test_closing_scope_removes_its_partition_only() -> None:
    backend_dict = BackendDict(ExampleBackend, "ec2")
    scope_registry.create("tenant-1")
    scope_registry.create("tenant-2")

    for scope_id, marker in [("tenant-1", "one"), ("tenant-2", "two")]:
        binding = bind_request(scope_id)
        try:
            backend_dict["123456789012"]["us-east-1"].items.append(marker)
        finally:
            unbind_request(binding)

    uid_1 = None
    binding = bind_request("tenant-1")
    try:
        uid_1 = current_partition_uid()
    finally:
        unbind_request(binding)

    assert scope_registry.close("tenant-1") is True
    assert uid_1 not in backend_dict._partitions
    # tenant-2 data is untouched
    binding = bind_request("tenant-2")
    try:
        assert backend_dict["123456789012"]["us-east-1"].items == ["two"]
    finally:
        unbind_request(binding)
    scope_registry.close("tenant-2")


def test_reset_wipes_every_partition() -> None:
    backend_dict = BackendDict(ExampleBackend, "ec2")
    scope_registry.create("tenant-1")
    binding = bind_request("tenant-1")
    try:
        backend_dict["123456789012"]["us-east-1"].items.append("x")
        assert len(backend_dict._partitions) == 2
    finally:
        unbind_request(binding)

    BackendDict.reset()
    assert backend_dict._partitions == {None: {}}


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


def test_concurrent_creates_produce_one_scope() -> None:
    registry = ScopeRegistry()
    barrier = threading.Barrier(10)
    results: list[bool] = []
    lock = threading.Lock()

    def create() -> None:
        barrier.wait()
        created = registry.create("same")
        with lock:
            results.append(created)

    threads = [Thread(target=create) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(True) == 1
    assert results.count(False) == 9


def test_concurrent_requests_for_same_scope_share_one_view() -> None:
    backend_dict = BackendDict(ExampleBackend, "ec2")
    scope_registry.create("tenant-1")
    barrier = threading.Barrier(12)
    backends: list[ExampleBackend] = []
    list_lock = threading.Lock()

    def hit() -> None:
        binding = bind_request("tenant-1")
        try:
            barrier.wait()
            backend = backend_dict["123456789012"]["us-east-1"]
            backend.items.append("x")
            with list_lock:
                backends.append(backend)
        finally:
            unbind_request(binding)

    threads = [Thread(target=hit) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len({id(b) for b in backends}) == 1
    assert len(backends[0].items) == 12


def test_concurrent_distinct_scopes_are_isolated() -> None:
    backend_dict = BackendDict(ExampleBackend, "ec2")
    scope_registry.create("tenant-1")
    scope_registry.create("tenant-2")

    def workload(scope_id: str, marker: str) -> None:
        for _ in range(50):
            binding = bind_request(scope_id)
            try:
                backend_dict["123456789012"]["us-east-1"].items.append(marker)
            finally:
                unbind_request(binding)

    t1 = Thread(target=workload, args=("tenant-1", "a"))
    t2 = Thread(target=workload, args=("tenant-2", "b"))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    binding = bind_request("tenant-1")
    try:
        backend_1 = backend_dict["123456789012"]["us-east-1"]
    finally:
        unbind_request(binding)
    binding = bind_request("tenant-2")
    try:
        backend_2 = backend_dict["123456789012"]["us-east-1"]
    finally:
        unbind_request(binding)

    assert backend_1.items == ["a"] * 50
    assert backend_2.items == ["b"] * 50


def test_close_waits_for_in_flight_requests_without_blocking_other_scopes() -> None:
    registry = ScopeRegistry()
    registry.create("busy")
    registry.create("other")

    request_entered = Event()
    release_request = Event()

    def hold_request() -> None:
        scope = registry.acquire("busy")
        request_entered.set()
        release_request.wait(5)
        registry.release(scope)

    worker = Thread(target=hold_request)
    worker.start()
    request_entered.wait(5)

    closed: list[bool] = []

    def close_busy() -> None:
        closed.append(registry.close("busy"))

    closer = Thread(target=close_busy)
    closer.start()

    # Give the closer a moment to start waiting
    assert not closer.is_alive() or True
    threading.Event().wait(0.2)
    assert closer.is_alive(), "close() must wait for the in-flight request"

    # Other scopes keep working while the close is pending: creation, requests
    # and releases all succeed.
    assert registry.create("third") is True
    scope = registry.acquire("other")
    registry.release(scope)
    scope = registry.acquire("third")
    registry.release(scope)

    release_request.set()
    worker.join()
    closer.join()

    assert closed == [True]
    with pytest.raises(ScopeNotFoundError):
        registry.acquire("busy")
    assert registry.exists("other")
    assert registry.exists("third")

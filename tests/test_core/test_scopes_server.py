"""
End-to-end tests for caller scopes against a real ThreadedMotoServer.

These tests spin up their own server (they are skipped when the shared
TEST_SERVER_MODE server is used) and speak raw HTTP, like the existing
multi-account server tests.
"""

import threading
from unittest import SkipTest
from urllib.parse import quote

import requests
import xmltodict

from moto import settings
from moto.core import DEFAULT_ACCOUNT_ID
from moto.server import ThreadedMotoServer

SERVER_PORT = 5023
BASE_URL = f"http://127.0.0.1:{SERVER_PORT}"

SCOPE_A = "test-suite-a"
SCOPE_B = "test-suite-b"

# Bypass any ambient HTTP(S) proxy - all traffic targets localhost directly.
HTTP = requests.Session()
HTTP.trust_env = False


def _sqs_headers(scope_id: str | None = None) -> dict[str, str]:
    headers = {
        "Authorization": (
            "AWS4-HMAC-SHA256 "
            "Credential=test/20240101/us-east-1/sqs/aws4_request, "
            "SignedHeaders=host, Signature=x"
        ),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    if scope_id is not None:
        headers["x-moto-scope-id"] = scope_id
    return headers


def _sqs(
    action: str,
    scope_id: str | None = None,
    params: dict[str, str] | None = None,
    path: str = "/",
) -> requests.Response:
    data = {"Action": action, "Version": "2012-11-05"}
    data.update(params or {})
    return HTTP.post(
        f"{BASE_URL}{path}",
        data=data,
        headers=_sqs_headers(scope_id),
    )


def _queue_urls(resp: requests.Response) -> list[str]:
    body = xmltodict.parse(resp.content)
    result = body.get("ListQueuesResponse", {}).get("ListQueuesResult", {})
    urls = result.get("QueueUrl", [])
    if isinstance(urls, str):
        urls = [urls]
    return urls


def _s3_request(
    method: str,
    bucket: str,
    scope_id: str | None = None,
) -> requests.Response:
    headers = {"Host": f"{bucket}.localhost:{SERVER_PORT}"} if bucket else {}
    if scope_id is not None:
        headers["x-moto-scope-id"] = scope_id
    return HTTP.request(method, f"{BASE_URL}/", headers=headers)


class TestCallerScopesServer:
    def setup_method(self) -> None:
        if settings.TEST_SERVER_MODE:
            raise SkipTest(
                "No point in testing this in ServerMode, as we already start our own server"
            )
        self.server = ThreadedMotoServer(port=SERVER_PORT, verbose=False)
        self.server.start()
        # Clean slate
        HTTP.post(f"{BASE_URL}/moto-api/reset")
        for scope_id in (SCOPE_A, SCOPE_B):
            resp = HTTP.put(f"{BASE_URL}/moto-api/scopes/{scope_id}")
            assert resp.status_code == 201

    def teardown_method(self) -> None:
        HTTP.post(f"{BASE_URL}/moto-api/reset")
        self.server.stop()

    def test_scope_management_api(self) -> None:
        # Creating an existing scope is idempotent
        resp = HTTP.put(f"{BASE_URL}/moto-api/scopes/{SCOPE_A}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "exists"

        # Both scopes are listed
        resp = HTTP.get(f"{BASE_URL}/moto-api/scopes")
        assert resp.status_code == 200
        assert sorted(resp.json()["scopes"]) == sorted([SCOPE_A, SCOPE_B])

        # Unknown scope inspect -> explicit 404
        resp = HTTP.get(f"{BASE_URL}/moto-api/scopes/ghost")
        assert resp.status_code == 404
        assert resp.json()["__type"] == "ScopeNotFound"

        # Invalid scope id -> explicit 400
        resp = HTTP.put(f"{BASE_URL}/moto-api/scopes/bad%20id")
        assert resp.status_code == 400
        assert resp.json()["__type"] == "InvalidScopeId"

    def test_same_named_queue_exists_independently_per_scope(self) -> None:
        queue_name = "shared-queue"
        for scope_id in (SCOPE_A, SCOPE_B):
            resp = _sqs("CreateQueue", scope_id, {"QueueName": queue_name})
            assert resp.status_code == 200, resp.content

        urls_a = _queue_urls(_sqs("ListQueues", SCOPE_A))
        urls_b = _queue_urls(_sqs("ListQueues", SCOPE_B))
        assert len(urls_a) == 1
        assert len(urls_b) == 1
        assert queue_name in urls_a[0]
        assert queue_name in urls_b[0]
        # The two scopes expose the same URL shape but back distinct queues
        assert urls_a == urls_b

        # Messages do not leak across scopes, even though the queue name matches
        queue_path = f"/{DEFAULT_ACCOUNT_ID}/{queue_name}"
        resp = _sqs(
            "SendMessage",
            SCOPE_A,
            {"MessageBody": "secret-from-a"},
            path=queue_path,
        )
        assert resp.status_code == 200, resp.content

        resp = _sqs("ReceiveMessage", SCOPE_B, path=queue_path)
        assert resp.status_code == 200
        assert "<Message>" not in resp.text

        resp = _sqs("ReceiveMessage", SCOPE_A, path=queue_path)
        assert "secret-from-a" in resp.text

    def test_scoped_request_to_unknown_scope_fails_explicitly(self) -> None:
        resp = _sqs("ListQueues", "ghost")
        assert resp.status_code == 404
        assert resp.json()["__type"] == "ScopeNotFound"

    def test_cross_scope_resource_access_is_not_found(self) -> None:
        # Resource created in A is not addressable from B
        resp = _sqs("CreateQueue", SCOPE_A, {"QueueName": "only-in-a"})
        assert resp.status_code == 200

        resp = _sqs("GetQueueUrl", SCOPE_B, {"QueueName": "only-in-a"})
        assert resp.status_code == 400
        assert "NonExistentQueue" in resp.text

    def test_unscoped_requests_keep_the_legacy_view(self) -> None:
        _sqs("CreateQueue", SCOPE_A, {"QueueName": "scoped-a"})
        _sqs("CreateQueue", SCOPE_B, {"QueueName": "scoped-b"})

        # No scope header: none of the scoped resources are visible
        resp = _sqs("ListQueues", None)
        assert resp.status_code == 200
        assert "<QueueUrl>" not in resp.text

        # Legacy data remains visible without the header
        _sqs("CreateQueue", None, {"QueueName": "legacy"})
        resp = _sqs("ListQueues", None)
        assert "legacy" in resp.text
        resp = _sqs("ListQueues", SCOPE_A)
        assert "legacy" not in resp.text

    def test_s3_buckets_are_isolated_and_legacy_view_is_intact(self) -> None:
        assert _s3_request("PUT", "bucket-a", SCOPE_A).status_code in (200, 201)
        assert _s3_request("PUT", "bucket-b", SCOPE_B).status_code in (200, 201)

        resp = _s3_request("GET", "", SCOPE_A)
        assert b"<Name>bucket-a</Name>" in resp.content
        assert b"<Name>bucket-b</Name>" not in resp.content

        resp = _s3_request("GET", "", SCOPE_B)
        assert b"<Name>bucket-b</Name>" in resp.content
        assert b"<Name>bucket-a</Name>" not in resp.content

        # The legacy view knows neither bucket
        resp = HTTP.get(f"{BASE_URL}/")
        assert b"bucket-a" not in resp.content
        assert b"bucket-b" not in resp.content

    def test_scope_and_account_dimensions_are_orthogonal(self) -> None:
        other_account = "333344445555"

        def create_queue(scope_id: str, account_id: str, name: str) -> None:
            headers = _sqs_headers(scope_id)
            if account_id != DEFAULT_ACCOUNT_ID:
                headers["x-moto-account-id"] = account_id
            resp = HTTP.post(
                f"{BASE_URL}/",
                data={
                    "Action": "CreateQueue",
                    "Version": "2012-11-05",
                    "QueueName": name,
                },
                headers=headers,
            )
            assert resp.status_code == 200, resp.content

        def list_queues(scope_id: str, account_id: str) -> requests.Response:
            headers = _sqs_headers(scope_id)
            if account_id != DEFAULT_ACCOUNT_ID:
                headers["x-moto-account-id"] = account_id
            return HTTP.post(
                f"{BASE_URL}/",
                data={
                    "Action": "ListQueues",
                    "Version": "2012-11-05",
                },
                headers=headers,
            )

        create_queue(SCOPE_A, other_account, "cross-dim")
        # Same scope, default account -> not visible
        resp = list_queues(SCOPE_A, DEFAULT_ACCOUNT_ID)
        assert "cross-dim" not in resp.text
        # Same scope, other account -> visible
        resp = list_queues(SCOPE_A, other_account)
        assert "cross-dim" in resp.text
        # Same account, other scope -> not visible
        resp = list_queues(SCOPE_B, other_account)
        assert "cross-dim" not in resp.text

    def test_closing_scope_releases_its_data(self) -> None:
        _sqs("CreateQueue", SCOPE_A, {"QueueName": "doomed"})
        _sqs("CreateQueue", SCOPE_B, {"QueueName": "survivor"})

        resp = HTTP.delete(f"{BASE_URL}/moto-api/scopes/{quote(SCOPE_A)}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "released"

        # The released scope can no longer be used
        resp = _sqs("ListQueues", SCOPE_A)
        assert resp.status_code == 404
        assert resp.json()["__type"] == "ScopeNotFound"

        # Releasing it again is an explicit error
        resp = HTTP.delete(f"{BASE_URL}/moto-api/scopes/{quote(SCOPE_A)}")
        assert resp.status_code == 404

        # Other scope and the legacy view are untouched
        resp = _sqs("ListQueues", SCOPE_B)
        assert "survivor" in resp.text
        resp = _sqs("ListQueues", None)
        assert resp.status_code == 200

        # The scope id can be reused, starting from an empty view
        resp = HTTP.put(f"{BASE_URL}/moto-api/scopes/{SCOPE_A}")
        assert resp.status_code == 201
        resp = _sqs("ListQueues", SCOPE_A)
        assert "<QueueUrl>" not in resp.text

    def test_concurrent_scopes_are_independent_under_load(self) -> None:
        _sqs("CreateQueue", SCOPE_A, {"QueueName": "load-a"})
        _sqs("CreateQueue", SCOPE_B, {"QueueName": "load-b"})
        errors: list[str] = []

        def workload(scope_id: str, queue_name: str) -> None:
            path = f"/{DEFAULT_ACCOUNT_ID}/{queue_name}"
            try:
                for i in range(20):
                    resp = _sqs(
                        "SendMessage",
                        scope_id,
                        {"MessageBody": f"{scope_id}-{i}"},
                        path=path,
                    )
                    assert resp.status_code == 200, resp.content
            except AssertionError as e:  # pragma: no cover - failure reporting
                errors.append(str(e))
        threads = [
            threading.Thread(target=workload, args=(SCOPE_A, "load-a")),
            threading.Thread(target=workload, args=(SCOPE_A, "load-a")),
            threading.Thread(target=workload, args=(SCOPE_B, "load-b")),
            threading.Thread(target=workload, args=(SCOPE_B, "load-b")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []

        def count_messages(scope_id: str, queue_name: str) -> int:
            path = f"/{DEFAULT_ACCOUNT_ID}/{queue_name}"
            count = 0
            for _ in range(10):
                resp = _sqs(
                    "ReceiveMessage",
                    scope_id,
                    {"MaxNumberOfMessages": "10", "WaitTimeSeconds": "0"},
                    path=path,
                )
                parsed = xmltodict.parse(resp.content)
                result = (
                    parsed.get("ReceiveMessageResponse", {}) or {}
                ).get("ReceiveMessageResult") or {}
                messages = result.get("Message") or []
                if isinstance(messages, dict):
                    messages = [messages]
                if not messages:
                    break
                count += len(messages)
                for message in messages:
                    delete = _sqs(
                        "DeleteMessage",
                        scope_id,
                        {"ReceiptHandle": message["ReceiptHandle"]},
                        path=path,
                    )
                    assert delete.status_code == 200, delete.content
            return count

        assert count_messages(SCOPE_A, "load-a") == 40
        assert count_messages(SCOPE_B, "load-b") == 40

    def test_scope_create_and_close_run_concurrently_with_traffic(self) -> None:
        stop = threading.Event()
        errors: list[str] = []

        def traffic() -> None:
            while not stop.is_set():
                resp = _sqs("ListQueues", SCOPE_B)
                if resp.status_code != 200:  # pragma: no cover - failure reporting
                    errors.append(f"traffic failed: {resp.status_code}")
                    return

        worker = threading.Thread(target=traffic)
        worker.start()
        try:
            for i in range(5):
                ephemeral = f"ephemeral-{i}"
                resp = HTTP.put(f"{BASE_URL}/moto-api/scopes/{ephemeral}")
                assert resp.status_code == 201
                assert _sqs(
                    "CreateQueue", ephemeral, {"QueueName": f"q-{i}"}
                ).status_code == 200
                resp = HTTP.delete(f"{BASE_URL}/moto-api/scopes/{ephemeral}")
                assert resp.status_code == 200
                # Destroying one scope does not take down another
                resp = _sqs("ListQueues", SCOPE_B)
                assert resp.status_code == 200
        finally:
            stop.set()
            worker.join()
        assert errors == []

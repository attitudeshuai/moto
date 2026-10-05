"""Reliable delivery pathway for S3 event notifications.

Historically S3 event notifications are delivered inline: the S3 model calls
SQS/SNS/Lambda/EventBridge directly while an object is being created or
deleted and every delivery error is swallowed. Nothing is recorded, nothing is
retried and an event that can never reach its target simply disappears.

This module adds an opt-in pathway on top of that fire-and-forget behaviour:

* Every delivery produces a :class:`DeliveryRecord` (source, target, number of
  attempts, last failure reason and current status). Records are queryable and
  have a configurable capacity, evicting the oldest records when full.
* Failed deliveries can be retried using a configurable backoff strategy.
* When the attempts are exhausted the event is moved to the dead-letter queue
  registered for the target; targets without a dead-letter queue are marked as
  ``UNDELIVERED``.
* Both retries and dead-lettering can be switched off explicitly. When they
  are, delivery behaves exactly as before: one inline attempt with all errors
  swallowed.
* Record writes and retry attempts can happen concurrently. Terminal status
  transitions are compare-and-swapped, so one event can never end up in two
  contradictory terminal states. Deleting a target (or reconfiguring/removing
  the bucket notification configuration) converges all in-flight retries.

The pathway is intentionally framework agnostic: callers hand it a zero
argument callable that performs the actual delivery. All knowledge about SQS,
SNS, ... lives in :mod:`moto.s3.notifications`.
"""

import copy
import json
import threading
import weakref
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from moto.moto_api._internal import mock_random as random


class DeliveryStatus(str, Enum):
    """Lifecycle states of a delivery."""

    #: Record created, the first attempt has not run yet.
    PENDING = "PENDING"
    #: An attempt is currently executing.
    IN_FLIGHT = "IN_FLIGHT"
    #: An attempt failed and a retry is scheduled.
    RETRYING = "RETRYING"
    #: Terminal: the target accepted the event.
    SUCCEEDED = "SUCCEEDED"
    #: Terminal: the event was moved to the target's dead-letter queue.
    DEAD_LETTERED = "DEAD_LETTERED"
    #: Terminal: attempts are exhausted and no dead-letter queue is configured.
    UNDELIVERED = "UNDELIVERED"
    #: Terminal: the delivery was converged because its target/source went away.
    CANCELLED = "CANCELLED"


TERMINAL_STATUSES = frozenset(
    {
        DeliveryStatus.SUCCEEDED,
        DeliveryStatus.DEAD_LETTERED,
        DeliveryStatus.UNDELIVERED,
        DeliveryStatus.CANCELLED,
    }
)


class BackoffStrategy(str, Enum):
    """Backoff strategies used between retry attempts."""

    #: Wait ``base_delay`` seconds after every failure.
    FIXED = "fixed"
    #: Wait ``base_delay * failed_attempts`` seconds.
    LINEAR = "linear"
    #: Wait ``base_delay * multiplier ** (failed_attempts - 1)`` seconds.
    EXPONENTIAL = "exponential"


class EvictionStrategy(str, Enum):
    #: Drop the oldest record when the store reaches its capacity.
    OLDEST = "oldest"
    #: Prefer dropping the oldest terminal record; fall back to the oldest one.
    OLDEST_TERMINAL_FIRST = "oldest_terminal_first"


@dataclass
class RetryPolicy:
    """Configurable retry behaviour."""

    enabled: bool = False
    max_attempts: int = 3
    backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    base_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("delays must be >= 0")
        if self.multiplier < 1:
            raise ValueError("multiplier must be >= 1")

    @property
    def effective_max_attempts(self) -> int:
        # Retries disabled means exactly one attempt - the original behaviour.
        return self.max_attempts if self.enabled else 1

    def delay_for(self, failed_attempts: int) -> float:
        """Delay before the ``failed_attempts + 1``-th attempt."""
        if self.backoff == BackoffStrategy.FIXED:
            delay = self.base_delay
        elif self.backoff == BackoffStrategy.LINEAR:
            delay = self.base_delay * failed_attempts
        else:
            delay = self.base_delay * (self.multiplier ** (failed_attempts - 1))
        return min(delay, self.max_delay)


def _utcnow_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


class DeliveryRecord:
    """A queryable record describing one event -> one target delivery."""

    def __init__(
        self,
        *,
        source_arn: str,
        target_arn: str,
        target_type: str,
        payload: Any,
        attempt: Callable[[], None],
        source_detail: dict[str, Any] | None = None,
        configuration_id: str | None = None,
        native_dlq_arn: str | None = None,
        max_attempts: int = 1,
    ):
        self.id = str(random.uuid4())
        self.source_arn = source_arn
        self.source_detail = dict(source_detail or {})
        self.target_arn = target_arn
        self.target_type = target_type
        self.configuration_id = configuration_id
        self.status: DeliveryStatus = DeliveryStatus.PENDING
        self.attempts = 0
        self.max_attempts = max_attempts
        self.last_failure_reason: str | None = None
        self.dlq_arn: str | None = None
        self.native_dlq_arn = native_dlq_arn
        self.created_at = _utcnow_iso()
        self.updated_at = self.created_at
        self.next_attempt_at: str | None = None
        self.attempt_history: list[dict[str, Any]] = []

        # Internal bookkeeping - never serialized.
        self._payload = payload
        self._attempt_callable = attempt
        self._timer: threading.Timer | None = None
        self._attempting = False
        self._cancel_requested = False

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @staticmethod
    def _format_error(error: BaseException) -> str:
        return f"{type(error).__name__}: {error}"

    def to_dict(self) -> dict[str, Any]:
        """Snapshot of the record safe to hand to callers."""
        return {
            "id": self.id,
            "source_arn": self.source_arn,
            "source": copy.deepcopy(self.source_detail),
            "target_arn": self.target_arn,
            "target_type": self.target_type,
            "configuration_id": self.configuration_id,
            "status": self.status.value,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "last_failure_reason": self.last_failure_reason,
            "dead_letter_queue_arn": self.dlq_arn,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "next_attempt_at": self.next_attempt_at,
            "attempt_history": copy.deepcopy(self.attempt_history),
        }


class DeliveryRecordStore:
    """Thread-safe, capacity bounded, filterable store of delivery records."""

    def __init__(
        self,
        max_records: int = 1000,
        eviction: EvictionStrategy = EvictionStrategy.OLDEST_TERMINAL_FIRST,
    ):
        if max_records < 1:
            raise ValueError("max_records must be >= 1")
        self._lock = threading.RLock()
        self._records: dict[str, DeliveryRecord] = {}
        self.max_records = max_records
        self.eviction = eviction

    def add(self, record: DeliveryRecord) -> list[DeliveryRecord]:
        """Add a record and return any records that were evicted."""
        with self._lock:
            self._records[record.id] = record
            evicted: list[DeliveryRecord] = []
            while len(self._records) > self.max_records:
                evicted.append(self._select_eviction_candidate())
            return evicted

    def _select_eviction_candidate(self) -> DeliveryRecord:
        # Caller holds the lock. Insertion order == oldest first.
        if self.eviction == EvictionStrategy.OLDEST_TERMINAL_FIRST:
            candidate = next(
                (record for record in self._records.values() if record.is_terminal),
                None,
            ) or next(iter(self._records.values()))
        else:
            candidate = next(iter(self._records.values()))
        self._records.pop(candidate.id)
        return candidate

    def get(self, record_id: str) -> DeliveryRecord | None:
        with self._lock:
            return self._records.get(record_id)

    def all(self) -> list[DeliveryRecord]:
        with self._lock:
            return list(self._records.values())

    def filter(
        self,
        *,
        source_arn: str | None = None,
        target_arn: str | None = None,
        target_type: str | None = None,
        status: DeliveryStatus | str | None = None,
    ) -> list[DeliveryRecord]:
        status_value = status.value if isinstance(status, DeliveryStatus) else status
        with self._lock:
            records = list(self._records.values())
        result = []
        for record in records:
            if source_arn is not None and record.source_arn != source_arn:
                continue
            if target_arn is not None and record.target_arn != target_arn:
                continue
            if target_type is not None and record.target_type != target_type:
                continue
            if status_value is not None and record.status.value != status_value:
                continue
            result.append(record)
        return result

    def clear(self) -> list[DeliveryRecord]:
        with self._lock:
            records = list(self._records.values())
            self._records.clear()
            return records

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)


# All live managers, so that deleting a target in another service (SQS, SNS,
# Lambda, EventBridge) can converge in-flight retries everywhere.
_MANAGERS: "weakref.WeakSet[DeliveryManager]" = weakref.WeakSet()
_MANAGERS_LOCK = threading.Lock()


def _register_manager(manager: "DeliveryManager") -> None:
    with _MANAGERS_LOCK:
        _MANAGERS.add(manager)


def cancel_target(target_arn: str) -> int:
    """Converge in-flight deliveries for a target ARN in every manager."""
    with _MANAGERS_LOCK:
        managers = list(_MANAGERS)
    return sum(manager.cancel_target(target_arn) for manager in managers)


def cancel_source(source_arn: str) -> int:
    """Converge in-flight deliveries originating from ``source_arn``."""
    with _MANAGERS_LOCK:
        managers = list(_MANAGERS)
    return sum(manager.cancel_source(source_arn) for manager in managers)


class DeliveryManager:
    """Coordinates records, retries, dead-lettering and convergence."""

    def __init__(
        self,
        region_name: str | None = None,
        account_id: str | None = None,
        *,
        max_records: int = 1000,
    ):
        self.region_name = region_name
        self.account_id = account_id
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._retry_policy = RetryPolicy()
        self._dead_letter_enabled = False
        self._dlq_registry: dict[str, str] = {}
        self._store = DeliveryRecordStore(max_records=max_records)
        #: Record ids that are not in a terminal state yet.
        self._active: set[str] = set()
        self._closed = False
        self._timer_factory = threading.Timer
        _register_manager(self)

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------
    def configure(
        self,
        *,
        retries_enabled: bool | None = None,
        max_attempts: int | None = None,
        backoff: str | BackoffStrategy | None = None,
        base_delay: float | None = None,
        max_delay: float | None = None,
        multiplier: float | None = None,
        dead_letter_enabled: bool | None = None,
        max_records: int | None = None,
        eviction: str | EvictionStrategy | None = None,
    ) -> "DeliveryManager":
        """Update any subset of the pathway configuration."""
        with self._lock:
            policy = self._retry_policy
            self._retry_policy = RetryPolicy(
                enabled=policy.enabled if retries_enabled is None else retries_enabled,
                max_attempts=policy.max_attempts
                if max_attempts is None
                else max_attempts,
                backoff=policy.backoff if backoff is None else BackoffStrategy(backoff),
                base_delay=policy.base_delay if base_delay is None else base_delay,
                max_delay=policy.max_delay if max_delay is None else max_delay,
                multiplier=policy.multiplier if multiplier is None else multiplier,
            )
            if dead_letter_enabled is not None:
                self._dead_letter_enabled = dead_letter_enabled
            if max_records is not None or eviction is not None:
                old_store = self._store
                self._store = DeliveryRecordStore(
                    max_records=(
                        old_store.max_records if max_records is None else max_records
                    ),
                    eviction=(
                        old_store.eviction
                        if eviction is None
                        else EvictionStrategy(eviction)
                    ),
                )
                for record in old_store.clear():
                    if record._timer is not None:
                        record._timer.cancel()
                        record._timer = None
                    for evicted_record in self._store.add(record):
                        self._converge_evicted(evicted_record)
        return self

    @property
    def retry_policy(self) -> RetryPolicy:
        with self._lock:
            return self._retry_policy

    @property
    def dead_letter_enabled(self) -> bool:
        with self._lock:
            return self._dead_letter_enabled

    def enable_retries(self, **kwargs: Any) -> "DeliveryManager":
        kwargs["retries_enabled"] = True
        return self.configure(**kwargs)

    def disable_retries(self) -> "DeliveryManager":
        return self.configure(retries_enabled=False)

    def enable_dead_letter(self) -> "DeliveryManager":
        return self.configure(dead_letter_enabled=True)

    def disable_dead_letter(self) -> "DeliveryManager":
        return self.configure(dead_letter_enabled=False)

    def register_dead_letter_queue(
        self, target_arn: str, dlq_arn: str
    ) -> "DeliveryManager":
        """Register the SQS dead-letter queue ARN for a delivery target."""
        self._validate_sqs_arn(dlq_arn)
        with self._lock:
            self._dlq_registry[target_arn] = dlq_arn
        return self

    def unregister_dead_letter_queue(self, target_arn: str) -> "DeliveryManager":
        with self._lock:
            self._dlq_registry.pop(target_arn, None)
        return self

    @staticmethod
    def _validate_sqs_arn(arn: str) -> None:
        parts = arn.split(":")
        if len(parts) != 6 or parts[0] != "arn" or parts[2] != "sqs":
            raise ValueError(f"Not a valid SQS queue ARN: {arn}")

    # ------------------------------------------------------------------
    # Submission / attempts
    # ------------------------------------------------------------------
    def submit(
        self,
        *,
        source_arn: str,
        target_arn: str,
        target_type: str,
        payload: Any,
        attempt: Callable[[], None],
        source_detail: dict[str, Any] | None = None,
        configuration_id: str | None = None,
        native_dlq_arn: str | None = None,
    ) -> DeliveryRecord | None:
        """Create a record and run the first attempt inline.

        Returns the record. Returns ``None`` only if the manager has been shut
        down (during a backend reset), in which case the attempt is executed
        once with all errors swallowed - i.e. the legacy behaviour.
        """
        with self._lock:
            if self._closed:
                self._swallow(attempt)
                return None
            record = DeliveryRecord(
                source_arn=source_arn,
                target_arn=target_arn,
                target_type=target_type,
                payload=payload,
                attempt=attempt,
                source_detail=source_detail,
                configuration_id=configuration_id,
                native_dlq_arn=native_dlq_arn,
                max_attempts=self._retry_policy.effective_max_attempts,
            )
            for evicted_record in self._store.add(record):
                self._converge_evicted(evicted_record)
            self._active.add(record.id)

        # The first attempt is always inline, preserving the synchronous
        # ordering that callers relied on before this pathway existed.
        self._execute_attempt(record.id)
        return record

    @staticmethod
    def _swallow(attempt: Callable[[], None]) -> None:
        try:
            attempt()
        except Exception:  # noqa: SIM105 - intentional, async delivery
            pass

    def _execute_attempt(self, record_id: str) -> None:
        with self._lock:
            record = self._store.get(record_id)
            if record is None:
                self._active.discard(record_id)
                self._condition.notify_all()
                return
            if record.is_terminal or self._closed or record._cancel_requested:
                self._set_terminal(record, DeliveryStatus.CANCELLED)
                return
            record.status = DeliveryStatus.IN_FLIGHT
            record._attempting = True
            attempt = record._attempt_callable

        error: BaseException | None = None
        try:
            attempt()
        except Exception as exc:  # noqa: BLE001 - delivery errors are recorded
            error = exc

        # Work decided while holding the lock, executed once it is released.
        dlq_arn: str | None = None
        dlq_body: str | None = None

        with self._lock:
            record = self._store.get(record_id)
            if record is None:
                self._active.discard(record_id)
                self._condition.notify_all()
                return
            record._attempting = False

            if self._closed or record._cancel_requested:
                self._set_terminal(record, DeliveryStatus.CANCELLED)
                return

            record.attempts += 1
            record.updated_at = _utcnow_iso()

            if error is None:
                self._set_terminal(record, DeliveryStatus.SUCCEEDED)
                return

            record.last_failure_reason = record._format_error(error)
            record.attempt_history.append(
                {
                    "attempt": record.attempts,
                    "at": record.updated_at,
                    "error": record.last_failure_reason,
                }
            )

            if record.attempts < record.max_attempts:
                delay = self._retry_policy.delay_for(record.attempts)
                record.status = DeliveryStatus.RETRYING
                record.next_attempt_at = datetime.fromtimestamp(
                    datetime.now(tz=timezone.utc).timestamp() + delay,
                    tz=timezone.utc,
                ).isoformat()
                self._schedule_retry(record, delay)
                return

            # Attempts exhausted.
            if self._dead_letter_enabled:
                dlq_arn = self._dlq_registry.get(
                    record.target_arn, record.native_dlq_arn
                )
            if dlq_arn:
                record.dlq_arn = dlq_arn
                dlq_body = self._build_dlq_envelope(record)
            else:
                self._set_terminal(record, DeliveryStatus.UNDELIVERED)
                return

        # Send to the dead-letter queue outside the manager lock, then settle
        # the terminal state under the lock.
        dlq_error = self._deliver_to_sqs(dlq_arn, dlq_body)  # type: ignore[arg-type]

        with self._lock:
            record = self._store.get(record_id)
            if record is None:
                self._active.discard(record_id)
                self._condition.notify_all()
                return
            if self._closed or record._cancel_requested or record.is_terminal:
                if not record.is_terminal:
                    self._set_terminal(record, DeliveryStatus.CANCELLED)
                return
            if dlq_error is None:
                record.dlq_arn = dlq_arn
                self._set_terminal(record, DeliveryStatus.DEAD_LETTERED)
            else:
                record.dlq_arn = None
                record.updated_at = _utcnow_iso()
                record.last_failure_reason = (
                    f"DeadLetterQueueError: {record._format_error(dlq_error)}"
                )
                self._set_terminal(record, DeliveryStatus.UNDELIVERED)

    def _schedule_retry(self, record: DeliveryRecord, delay: float) -> None:
        # Caller holds the lock.
        timer = self._timer_factory(delay, self._execute_attempt, args=(record.id,))
        timer.daemon = True
        record._timer = timer
        timer.start()

    @staticmethod
    def _build_dlq_envelope(record: DeliveryRecord) -> str:
        return json.dumps(
            {
                "deliveryRecordId": record.id,
                "eventSourceArn": record.source_arn,
                "eventSource": record.source_detail,
                "targetArn": record.target_arn,
                "targetType": record.target_type,
                "configurationId": record.configuration_id,
                "attempts": record.attempts,
                "failureReason": record.last_failure_reason,
                "event": record._payload,
            }
        )

    @staticmethod
    def _deliver_to_sqs(queue_arn: str, body: str) -> BaseException | None:
        try:
            from moto.sqs.models import sqs_backends

            _, _, _, region, account_id, queue_name = queue_arn.split(":", 5)
            sqs_backend = sqs_backends[account_id][region]
            sqs_backend.send_message(queue_name=queue_name, message_body=body)
            return None
        except Exception as exc:  # noqa: BLE001 - reported via the record
            return exc

    def _set_terminal(self, record: DeliveryRecord, status: DeliveryStatus) -> None:
        # Caller holds the lock. Compare-and-swap: a terminal decision is final.
        if record.is_terminal:
            return
        record.status = status
        record.updated_at = _utcnow_iso()
        record._timer = None
        record.next_attempt_at = None
        self._active.discard(record.id)
        self._condition.notify_all()

    def _converge_evicted(self, record: DeliveryRecord) -> None:
        # Caller holds the lock.
        if record._timer is not None:
            record._timer.cancel()
            record._timer = None
        record._cancel_requested = True
        self._active.discard(record.id)
        self._condition.notify_all()

    # ------------------------------------------------------------------
    # Convergence
    # ------------------------------------------------------------------
    def cancel_target(self, target_arn: str) -> int:
        """Cancel all non-terminal deliveries for one target."""
        with self._lock:
            return self._cancel(lambda record: record.target_arn == target_arn)

    def cancel_source(self, source_arn: str) -> int:
        """Cancel all non-terminal deliveries originating from one source."""
        with self._lock:
            return self._cancel(lambda record: record.source_arn == source_arn)

    def _cancel(self, predicate: Callable[[DeliveryRecord], bool]) -> int:
        # Caller holds the lock.
        if self._closed:
            return 0
        cancelled = 0
        for record in self._store.all():
            if record.is_terminal or not predicate(record):
                continue
            record._cancel_requested = True
            if record._timer is not None:
                record._timer.cancel()
                record._timer = None
            if not record._attempting:
                # Nothing in flight - converge immediately. An in-flight
                # attempt converges itself to CANCELLED when it returns.
                self._set_terminal(record, DeliveryStatus.CANCELLED)
            cancelled += 1
        return cancelled

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------
    def list_records(
        self,
        *,
        source_arn: str | None = None,
        target_arn: str | None = None,
        target_type: str | None = None,
        status: DeliveryStatus | str | None = None,
    ) -> list[dict[str, Any]]:
        return [
            record.to_dict()
            for record in self._store.filter(
                source_arn=source_arn,
                target_arn=target_arn,
                target_type=target_type,
                status=status,
            )
        ]

    def get_record(self, record_id: str) -> dict[str, Any] | None:
        record = self._store.get(record_id)
        return record.to_dict() if record else None

    def wait_idle(self, timeout: float = 10.0) -> bool:
        """Wait until every recorded delivery reached a terminal state."""
        with self._condition:
            return self._condition.wait_for(
                lambda: not self._active or self._closed, timeout=timeout
            )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        """Cancel every retry and stop accepting deliveries."""
        with self._lock:
            self._closed = True
            for record in self._store.all():
                record._cancel_requested = True
                if record._timer is not None:
                    record._timer.cancel()
                    record._timer = None
                if not record._attempting and not record.is_terminal:
                    record.status = DeliveryStatus.CANCELLED
                    record.updated_at = _utcnow_iso()
                    record.next_attempt_at = None
            self._active.clear()
            self._condition.notify_all()

    def reset(self) -> None:
        self.shutdown()
        with self._lock:
            self._store.clear()
            self._dlq_registry.clear()
            self._closed = False
            self._retry_policy = RetryPolicy()
            self._dead_letter_enabled = False


__all__ = [
    "BackoffStrategy",
    "DeliveryManager",
    "DeliveryRecord",
    "DeliveryRecordStore",
    "DeliveryStatus",
    "EvictionStrategy",
    "RetryPolicy",
    "TERMINAL_STATUSES",
    "cancel_source",
    "cancel_target",
]

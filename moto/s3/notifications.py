import copy
import json
from collections.abc import Callable
from enum import Enum
from typing import TYPE_CHECKING, Any
from urllib.parse import quote_plus

from moto.core.utils import iso_8601_datetime_with_milliseconds, unix_time

from .delivery import DeliveryManager

if TYPE_CHECKING:
    from moto.events.utils import EventMessageType
    from moto.s3.models import FakeBucket


class S3NotificationEvent(str, Enum):
    REDUCED_REDUNDANCY_LOST_OBJECT_EVENT = "s3:ReducedRedundancyLostObject"
    OBJECT_CREATED_EVENT = "s3:ObjectCreated:*"
    OBJECT_CREATED_PUT_EVENT = "s3:ObjectCreated:Put"
    OBJECT_CREATED_POST_EVENT = "s3:ObjectCreated:Post"
    OBJECT_CREATED_COPY_EVENT = "s3:ObjectCreated:Copy"
    OBJECT_CREATED_COMPLETE_MULTIPART_UPLOAD_EVENT = (
        "s3:ObjectCreated:CompleteMultipartUpload"
    )
    OBJECT_REMOVED_EVENT = "s3:ObjectRemoved:*"
    OBJECT_REMOVED_DELETE_EVENT = "s3:ObjectRemoved:Delete"
    OBJECT_REMOVED_DELETE_MARKER_CREATED_EVENT = "s3:ObjectRemoved:DeleteMarkerCreated"
    OBJECT_RESTORE_EVENT = "s3:ObjectRestore:*"
    OBJECT_RESTORE_POST_EVENT = "s3:ObjectRestore:Post"
    OBJECT_RESTORE_COMPLETED_EVENT = "s3:ObjectRestore:Completed"
    REPLICATION_EVENT = "s3:Replication:*"
    REPLICATION_OPERATION_FAILED_REPLICATION_EVENT = (
        "s3:Replication:OperationFailedReplication"
    )
    REPLICATION_OPERATION_NOT_TRACKED_EVENT = "s3:Replication:OperationNotTracked"
    REPLICATION_OPERATION_MISSED_THRESHOLD_EVENT = (
        "s3:Replication:OperationMissedThreshold"
    )
    REPLICATION_OPERATION_REPLICATED_AFTER_THRESHOLD_EVENT = (
        "s3:Replication:OperationReplicatedAfterThreshold"
    )
    OBJECT_RESTORE_DELETE_EVENT = "s3:ObjectRestore:Delete"
    LIFECYCLE_TRANSITION_EVENT = "s3:LifecycleTransition"
    INTELLIGENT_TIERING_EVENT = "s3:IntelligentTiering"
    OBJECT_ACL_UPDATE_EVENT = "s3:ObjectAcl:Put"
    LIFECYCLE_EXPIRATION_EVENT = "s3:LifecycleExpiration:*"
    LIFECYCLEEXPIRATION_DELETE_EVENT = "s3:LifecycleExpiration:Delete"
    LIFECYCLE_EXPIRATION_DELETE_MARKER_CREATED_EVENT = (
        "s3:LifecycleExpiration:DeleteMarkerCreated"
    )
    OBJECT_TAGGING_EVENT = "s3:ObjectTagging:*"
    OBJECT_TAGGING_PUT_EVENT = "s3:ObjectTagging:Put"
    OBJECT_TAGGING_DELETE_EVENT = "s3:ObjectTagging:Delete"

    @classmethod
    def events(self) -> list[str]:
        return sorted([item.value for item in S3NotificationEvent])

    @classmethod
    def is_event_valid(self, event_name: str) -> bool:
        # Ex) s3:ObjectCreated:Put
        if event_name in self.events():
            return True
        # Ex) event name without `s3:` like ObjectCreated:Put
        if event_name in [e[:3] for e in self.events()]:
            return True
        return False


def _get_s3_event(
    event_name: str, bucket: "FakeBucket", key: Any, notification_id: str
) -> dict[str, list[dict[str, Any]]]:
    etag = key.etag.replace('"', "")
    # s3:ObjectCreated:Put --> ObjectCreated:Put
    event_name = event_name[3:]
    event_time = iso_8601_datetime_with_milliseconds()
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
    key_name = quote_plus(key.name)
    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": bucket.region_name,
                "eventTime": event_time,
                "eventName": event_name,
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": notification_id,
                    "bucket": {
                        "name": bucket.name,
                        "arn": bucket.arn,
                    },
                    "object": {"key": key_name, "size": key.size, "eTag": etag},
                },
            }
        ]
    }


def _get_region_from_arn(arn: str) -> str:
    return arn.split(":")[3]


def _get_delivery_manager(bucket: "FakeBucket") -> DeliveryManager | None:
    # Lazy import to avoid the circular import s3.models <-> s3.notifications.
    # S3 backends are partition scoped, not region scoped.
    try:
        from moto.s3.models import s3_backends

        return s3_backends[bucket.account_id][bucket.partition].delivery_manager
    except Exception:  # noqa: BLE001 - never break an object operation
        return None


def _source_detail(bucket: "FakeBucket", event_name: Any, key: Any) -> dict[str, Any]:
    return {
        "service": "s3",
        "bucket": bucket.name,
        "region": bucket.region_name,
        "event": getattr(event_name, "value", event_name),
        "key": key.name,
    }


def send_event(
    account_id: str, event_name: S3NotificationEvent, bucket: Any, key: Any
) -> None:
    if bucket.notification_configuration is None:
        return

    manager = _get_delivery_manager(bucket)
    if manager is None:
        # No backend attached (e.g. standalone use) - fall back to the
        # legacy fire-and-forget behaviour.
        _send_event_legacy(account_id, event_name, bucket, key)
        return

    source_arn = bucket.arn
    source_detail = _source_detail(bucket, event_name, key)

    for notification in bucket.notification_configuration.cloud_function:
        if not notification.matches(event_name, key.name):
            continue
        event_body = _get_s3_event(event_name, bucket, key, notification.id)
        region_name = _get_region_from_arn(notification.arn)
        manager.submit(
            source_arn=source_arn,
            source_detail=source_detail,
            target_arn=notification.arn,
            target_type="lambda",
            payload=event_body,
            configuration_id=notification.id,
            attempt=_lambda_attempt(
                account_id, event_body, notification.arn, region_name
            ),
        )

    for notification in bucket.notification_configuration.queue:
        if not notification.matches(event_name, key.name):
            continue
        event_body = _get_s3_event(event_name, bucket, key, notification.id)
        region_name = _get_region_from_arn(notification.arn)
        queue_name = notification.arn.split(":")[-1]
        manager.submit(
            source_arn=source_arn,
            source_detail=source_detail,
            target_arn=notification.arn,
            target_type="sqs",
            payload=event_body,
            configuration_id=notification.id,
            native_dlq_arn=_native_sqs_dead_letter_arn(
                account_id, region_name, queue_name
            ),
            attempt=_sqs_attempt(account_id, event_body, queue_name, region_name),
        )

    for notification in bucket.notification_configuration.topic:
        if not notification.matches(event_name, key.name):
            continue
        event_body = _get_s3_event(event_name, bucket, key, notification.id)
        region_name = _get_region_from_arn(notification.arn)
        manager.submit(
            source_arn=source_arn,
            source_detail=source_detail,
            target_arn=notification.arn,
            target_type="sns",
            payload=event_body,
            configuration_id=notification.id,
            attempt=_sns_attempt(account_id, event_body, notification.arn, region_name),
        )

    if bucket.notification_configuration.event_bridge is not None:
        _send_event_bridge_via_manager(manager, account_id, bucket, event_name, key)


def _send_event_legacy(
    account_id: str, event_name: S3NotificationEvent, bucket: Any, key: Any
) -> None:
    for notification in bucket.notification_configuration.cloud_function:
        if notification.matches(event_name, key.name):
            event_body = _get_s3_event(event_name, bucket, key, notification.id)
            region_name = _get_region_from_arn(notification.arn)
            _invoke_awslambda(account_id, event_body, notification.arn, region_name)

    for notification in bucket.notification_configuration.queue:
        if notification.matches(event_name, key.name):
            event_body = _get_s3_event(event_name, bucket, key, notification.id)
            region_name = _get_region_from_arn(notification.arn)
            queue_name = notification.arn.split(":")[-1]
            _send_sqs_message(account_id, event_body, queue_name, region_name)

    for notification in bucket.notification_configuration.topic:
        if notification.matches(event_name, key.name):
            event_body = _get_s3_event(event_name, bucket, key, notification.id)
            region_name = _get_region_from_arn(notification.arn)
            _send_sns_message(account_id, event_body, notification.arn, region_name)

    if bucket.notification_configuration.event_bridge is not None:
        _send_event_bridge_message(account_id, bucket, event_name, key)


# ---------------------------------------------------------------------------
# Attempt factories. Each returns a zero-argument callable that raises on
# failure. The delivery manager records the result and handles retries/DLQ.
# ---------------------------------------------------------------------------
def _sqs_attempt(
    account_id: str, event_body: Any, queue_name: str, region_name: str
) -> Callable[[], None]:
    def attempt() -> None:
        from moto.sqs.models import sqs_backends

        sqs_backend = sqs_backends[account_id][region_name]
        sqs_backend.send_message(
            queue_name=queue_name, message_body=json.dumps(event_body)
        )

    return attempt


def _sns_attempt(
    account_id: str, event_body: Any, topic_arn: str, region_name: str
) -> Callable[[], None]:
    def attempt() -> None:
        from moto.sns.models import sns_backends

        sns_backend = sns_backends[account_id][region_name]
        sns_backend.publish(arn=topic_arn, message=json.dumps(event_body))

    return attempt


def _lambda_attempt(
    account_id: str, event_body: Any, fn_arn: str, region_name: str
) -> Callable[[], None]:
    def attempt() -> None:
        from moto.awslambda.utils import get_backend

        lambda_backend = get_backend(account_id, region_name)
        func = lambda_backend.get_function(fn_arn)
        func.invoke(json.dumps(event_body), {}, {})

    return attempt


def _native_sqs_dead_letter_arn(
    account_id: str, region_name: str, queue_name: str
) -> str | None:
    """The DLQ a queue itself registered via its redrive policy, if any."""
    try:
        from moto.sqs.models import sqs_backends

        queue = sqs_backends[account_id][region_name].queues.get(queue_name)
        if queue is not None and queue.redrive_policy is not None:
            return queue.redrive_policy.get("deadLetterTargetArn")
    except Exception:  # noqa: BLE001 - best effort lookup only
        return None
    return None


def _send_event_bridge_via_manager(
    manager: DeliveryManager,
    account_id: str,
    bucket: "FakeBucket",
    event_name: str,
    key: Any,
) -> None:
    try:
        event = _build_eventbridge_event(account_id, bucket, event_name, key)
    except Exception:  # noqa: BLE001 - unsupported events are dropped, as before
        return

    try:
        from moto.events.models import events_backends

        events_backend = events_backends[account_id][bucket.region_name]
        buses = list(events_backend.event_buses.values())
    except Exception:  # noqa: BLE001 - EventBridge not available behaves as no-op
        return

    for event_bus in buses:
        for rule in list(event_bus.rules.values()):
            try:
                if not rule.event_pattern.matches_event(event):
                    continue
            except Exception:  # noqa: BLE001 - a broken rule never breaks others
                continue
            for target in list(rule.targets):
                target_arn = target.get("Arn", "")
                native_dlq_arn = (target.get("DeadLetterConfig") or {}).get("Arn")
                manager.submit(
                    source_arn=bucket.arn,
                    source_detail=_source_detail(bucket, event_name, key),
                    target_arn=target_arn,
                    target_type="eventbridge",
                    payload=event,
                    configuration_id=rule.name,
                    native_dlq_arn=native_dlq_arn,
                    attempt=_eventbridge_target_attempt(rule, target, event),
                )


def _eventbridge_target_attempt(
    rule: Any, target: dict[str, Any], event: "EventMessageType"
) -> Callable[[], None]:
    def attempt() -> None:
        rule.send_to_target(target, event, transform_input=False)

    return attempt


def _send_sqs_message(
    account_id: str, event_body: Any, queue_name: str, region_name: str
) -> None:
    try:
        _sqs_attempt(account_id, event_body, queue_name, region_name)()
    except:  # noqa
        # This is an async action in AWS.
        # Even if this part fails, the calling function should pass, so catch all errors
        # Possible exceptions that could be thrown:
        # - Queue does not exist
        pass


def _send_sns_message(
    account_id: str, event_body: Any, topic_arn: str, region_name: str
) -> None:
    try:
        _sns_attempt(account_id, event_body, topic_arn, region_name)()
    except:  # noqa
        # This is an async action in AWS.
        # Even if this part fails, the calling function should pass, so catch all errors
        # Possible exceptions that could be thrown:
        # - Topic does not exist
        pass


def _build_eventbridge_event(
    account_id: str,
    bucket: "FakeBucket",
    event_name: str,
    key: Any,
) -> "EventMessageType":
    from moto.events.utils import _BASE_EVENT_MESSAGE

    event = copy.deepcopy(_BASE_EVENT_MESSAGE)
    event["detail-type"] = _detail_type(event_name)
    event["source"] = "aws.s3"
    event["account"] = account_id
    event["time"] = unix_time()
    event["region"] = bucket.region_name
    event["resources"] = [bucket.arn]
    event["detail"] = {
        "version": "0",
        "bucket": {"name": bucket.name},
        "object": {
            "key": key.name,
            "size": key.size,
            "eTag": key.etag.replace('"', ""),
            "version-id": key.version_id,
            "sequencer": "617f08299329d189",
        },
        "request-id": "N4N7GDK58NMKJ12R",
        "requester": "123456789012",
        "source-ip-address": "1.2.3.4",
        # ex) s3:ObjectCreated:Put -> ObjectCreated
        "reason": event_name.split(":")[1],
    }
    return event


def _send_event_bridge_message(
    account_id: str,
    bucket: "FakeBucket",
    event_name: str,
    key: Any,
) -> None:
    try:
        from moto.events.models import events_backends

        event = _build_eventbridge_event(account_id, bucket, event_name, key)

        events_backend = events_backends[account_id][bucket.region_name]
        for event_bus in events_backend.event_buses.values():
            for rule in event_bus.rules.values():
                rule.send_to_targets(event, transform_input=False)

    except:  # noqa
        # This is an async action in AWS.
        # Even if this part fails, the calling function should pass, so catch all errors
        # Possible exceptions that could be thrown:
        # - EventBridge does not exist
        pass


def _detail_type(event_name: str) -> str:
    """Detail type field values for event messages of s3 EventBridge notification

    document: https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventBridge.html
    """
    if event_name in [e for e in S3NotificationEvent.events() if "ObjectCreated" in e]:
        return "Object Created"
    elif event_name in [
        e
        for e in S3NotificationEvent.events()
        if "ObjectRemoved" in e or "LifecycleExpiration" in e
    ]:
        return "Object Deleted"
    elif event_name in [
        e for e in S3NotificationEvent.events() if "ObjectRestore" in e
    ]:
        if event_name == S3NotificationEvent.OBJECT_RESTORE_POST_EVENT:
            return "Object Restore Initiated"
        elif event_name == S3NotificationEvent.OBJECT_RESTORE_COMPLETED_EVENT:
            return "Object Restore Completed"
        else:
            # s3:ObjectRestore:Delete event
            return "Object Restore Expired"
    elif event_name in [
        e for e in S3NotificationEvent.events() if "LifecycleTransition" in e
    ]:
        return "Object Storage Class Changed"
    elif event_name in [
        e for e in S3NotificationEvent.events() if "IntelligentTiering" in e
    ]:
        return "Object Access Tier Changed"
    elif event_name in [e for e in S3NotificationEvent.events() if "ObjectAcl" in e]:
        return "Object ACL Updated"
    elif event_name in [e for e in S3NotificationEvent.events() if "ObjectTagging"]:
        if event_name == S3NotificationEvent.OBJECT_TAGGING_PUT_EVENT:
            return "Object Tags Added"
        else:
            # s3:ObjectTagging:Delete event
            return "Object Tags Deleted"
    else:
        raise ValueError(
            f"unsupported event `{event_name}` for s3 eventbridge notification (https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventBridge.html)"
        )


def _invoke_awslambda(
    account_id: str, event_body: Any, fn_arn: str, region_name: str
) -> None:
    try:
        _lambda_attempt(account_id, event_body, fn_arn, region_name)()
    except:  # noqa
        # This is an async action in AWS.
        # Even if this part fails, the calling function should pass, so catch all errors
        # Possible exceptions that could be thrown:
        # - Function does not exist
        pass


def _get_test_event(bucket_name: str) -> dict[str, Any]:
    event_time = iso_8601_datetime_with_milliseconds()
    return {
        "Service": "Amazon S3",
        "Event": "s3:TestEvent",
        "Time": event_time,
        "Bucket": bucket_name,
    }


def send_test_event(account_id: str, bucket: Any) -> None:
    arns = [n.arn for n in bucket.notification_configuration.queue]
    for arn in set(arns):
        region_name = _get_region_from_arn(arn)
        queue_name = arn.split(":")[-1]
        message_body = _get_test_event(bucket.name)
        _send_sqs_message(account_id, message_body, queue_name, region_name)

    arns = [n.arn for n in bucket.notification_configuration.topic]
    for arn in set(arns):
        region_name = _get_region_from_arn(arn)
        message_body = _get_test_event(bucket.name)
        _send_sns_message(account_id, message_body, arn, region_name)

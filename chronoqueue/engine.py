"""
ChronoQueue Core Engine
Implements standard SQS operations, FIFO sequencing, redrive policies, and adaptive visibility.
"""

from __future__ import annotations
import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from .models import Message, QueueConfig
from .storage import Storage

logger = logging.getLogger("chronoqueue.engine")


class QueueEngine:
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage or Storage()
        self._poll_condition = threading.Condition()

    def create_queue(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> QueueConfig:
        existing = self.storage.get_queue(name)
        if existing:
            return existing

        attrs = attributes or {}
        is_fifo = name.endswith(".fifo") or bool(attrs.get("FifoQueue", False))
        config = QueueConfig(
            name=name,
            is_fifo=is_fifo,
            visibility_timeout=int(attrs.get("VisibilityTimeout", 30)),
            message_retention_period=int(attrs.get("MessageRetentionPeriod", 345600)),
            delay_seconds=int(attrs.get("DelaySeconds", 0)),
            max_message_size=int(attrs.get("MaximumMessageSize", 262144)),
            dead_letter_target=attrs.get("RedrivePolicy", {}).get("deadLetterTargetArn") if isinstance(attrs.get("RedrivePolicy"), dict) else attrs.get("DeadLetterTarget"),
            max_receive_count=int(attrs.get("RedrivePolicy", {}).get("maxReceiveCount", 3)) if isinstance(attrs.get("RedrivePolicy"), dict) else int(attrs.get("MaxReceiveCount", 3)),
            content_based_deduplication=bool(attrs.get("ContentBasedDeduplication", False))
        )
        self.storage.save_queue(config)
        return config

    def get_queue(self, name: str) -> Optional[QueueConfig]:
        return self.storage.get_queue(name)

    def list_queues(self) -> List[QueueConfig]:
        return self.storage.list_queues()

    def delete_queue(self, name: str) -> bool:
        return self.storage.delete_queue(name)

    def purge_queue(self, name: str):
        self.storage.purge_queue(name)

    def send_message(
        self,
        queue_name: str,
        body: str,
        delay_seconds: Optional[int] = None,
        message_attributes: Optional[Dict[str, Any]] = None,
        message_group_id: Optional[str] = None,
        message_deduplication_id: Optional[str] = None,
        replayed_from_id: Optional[str] = None,
    ) -> Message:
        config = self.storage.get_queue(queue_name)
        if not config:
            # Auto-create if not present
            config = self.create_queue(queue_name)

        if len(body.encode("utf-8")) > config.max_message_size:
            raise ValueError(f"Message body exceeds maximum size of {config.max_message_size} bytes")

        now = time.time()
        effective_delay = delay_seconds if delay_seconds is not None else config.delay_seconds
        visible_after = now + effective_delay

        # FIFO Deduplication Window Check (5-minute sliding window)
        if config.is_fifo:
            dedup_id = message_deduplication_id
            if not dedup_id and config.content_based_deduplication:
                import hashlib
                dedup_id = hashlib.sha256(body.encode("utf-8")).hexdigest()

            if dedup_id:
                existing = self.storage.find_dedup_message(queue_name, dedup_id, window_seconds=300)
                if existing:
                    # AWS SQS returns the original message ID on duplicate within window
                    return existing

        msg = Message(
            queue_name=queue_name,
            body=body,
            message_attributes=message_attributes or {},
            sent_timestamp=now,
            visible_after=visible_after,
            message_group_id=message_group_id,
            message_deduplication_id=message_deduplication_id,
            replayed_from_id=replayed_from_id
        )

        self.storage.insert_message(msg)

        # Notify waiting long-poll consumers
        with self._poll_condition:
            self._poll_condition.notify_all()

        return msg

    def receive_messages(
        self,
        queue_name: str,
        max_number_of_messages: int = 1,
        visibility_timeout: Optional[int] = None,
        wait_time_seconds: int = 0
    ) -> List[Message]:
        config = self.storage.get_queue(queue_name)
        if not config:
            raise ValueError(f"Queue {queue_name} does not exist")

        timeout = visibility_timeout if visibility_timeout is not None else config.visibility_timeout
        max_msgs = max(1, min(max_number_of_messages, 10))
        deadline = time.time() + max(0, min(wait_time_seconds, 20))

        while True:
            now = time.time()
            candidates = self.storage.fetch_available_messages(queue_name, limit=max_msgs * 2, now=now)
            ready_messages: List[Message] = []

            for msg in candidates:
                # Check dead letter threshold
                if config.dead_letter_target and msg.receive_count >= config.max_receive_count:
                    self._route_to_dlq(msg, config.dead_letter_target, "Max receive count exceeded")
                    continue

                msg.receive_count += 1
                if not msg.first_received_timestamp:
                    msg.first_received_timestamp = now
                msg.status = "in_flight"
                msg.visible_after = now + timeout
                msg.generate_receipt_handle()
                self.storage.update_message(msg)
                ready_messages.append(msg)

                if len(ready_messages) >= max_msgs:
                    break

            if ready_messages:
                return ready_messages

            # Wait if long polling requested and time remains
            time_left = deadline - time.time()
            if time_left <= 0:
                return []

            with self._poll_condition:
                self._poll_condition.wait(timeout=min(time_left, 1.0))

    def _route_to_dlq(self, msg: Message, dlq_name: str, reason: str, error_context: Optional[Dict[str, Any]] = None):
        msg.status = "dead_letter"
        msg.dead_letter_reason = reason
        if error_context:
            msg.error_context = error_context
        self.storage.update_message(msg)

        # Ensure DLQ exists and insert copy into DLQ
        dlq_config = self.storage.get_queue(dlq_name)
        if not dlq_config:
            self.create_queue(dlq_name)

        dlq_msg = Message(
            queue_name=dlq_name,
            body=msg.body,
            message_attributes=msg.message_attributes,
            sent_timestamp=time.time(),
            visible_after=time.time(),
            status="available",
            dead_letter_reason=reason,
            error_context=msg.error_context or {"source_queue": msg.queue_name, "original_message_id": msg.message_id}
        )
        self.storage.insert_message(dlq_msg)

    def delete_message(self, queue_name: str, receipt_handle: str) -> bool:
        msg = self.storage.get_message_by_receipt(receipt_handle)
        if not msg or msg.queue_name != queue_name:
            return False

        msg.status = "deleted"
        msg.receipt_handle = None
        self.storage.update_message(msg)
        return True

    def change_message_visibility(self, queue_name: str, receipt_handle: str, visibility_timeout: int) -> bool:
        msg = self.storage.get_message_by_receipt(receipt_handle)
        if not msg or msg.queue_name != queue_name:
            return False

        msg.visible_after = time.time() + visibility_timeout
        self.storage.update_message(msg)
        return True

    def heartbeat(self, queue_name: str, receipt_handle: str, extend_by_seconds: int = 30) -> bool:
        """
        Adaptive visibility: Consumer heartbeat to prevent premature visibility timeout.
        """
        return self.change_message_visibility(queue_name, receipt_handle, extend_by_seconds)

    def report_failure(self, queue_name: str, receipt_handle: str, error_msg: str, stack_trace: Optional[str] = None) -> bool:
        """
        Novel capability: Consumer explicitly reports execution failure with contextual error details.
        """
        msg = self.storage.get_message_by_receipt(receipt_handle)
        if not msg or msg.queue_name != queue_name:
            return False

        config = self.storage.get_queue(queue_name)
        error_context = {
            "error": error_msg,
            "stack_trace": stack_trace,
            "failed_at": time.time(),
            "attempt": msg.receive_count
        }

        if config and config.dead_letter_target and msg.receive_count >= config.max_receive_count:
            self._route_to_dlq(msg, config.dead_letter_target, error_msg, error_context)
            return True

        # Make immediately visible again for retry with error attached
        msg.error_context = error_context
        msg.status = "available"
        msg.visible_after = time.time()
        self.storage.update_message(msg)
        return True

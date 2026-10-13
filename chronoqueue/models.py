"""
ChronoQueue Data Models
Defines internal representations for messages, queues, configurations, and novel telemetry.
"""

from __future__ import annotations
import hashlib
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class QueueConfig:
    name: str
    is_fifo: bool = False
    visibility_timeout: int = 30  # seconds
    message_retention_period: int = 345600  # 4 days
    delay_seconds: int = 0
    max_message_size: int = 262144  # 256 KB
    dead_letter_target: Optional[str] = None
    max_receive_count: int = 3
    content_based_deduplication: bool = False
    created_timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "is_fifo": self.is_fifo,
            "visibility_timeout": self.visibility_timeout,
            "message_retention_period": self.message_retention_period,
            "delay_seconds": self.delay_seconds,
            "max_message_size": self.max_message_size,
            "dead_letter_target": self.dead_letter_target,
            "max_receive_count": self.max_receive_count,
            "content_based_deduplication": self.content_based_deduplication,
            "created_timestamp": self.created_timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> QueueConfig:
        return cls(
            name=data["name"],
            is_fifo=data.get("is_fifo", False),
            visibility_timeout=int(data.get("visibility_timeout", 30)),
            message_retention_period=int(data.get("message_retention_period", 345600)),
            delay_seconds=int(data.get("delay_seconds", 0)),
            max_message_size=int(data.get("max_message_size", 262144)),
            dead_letter_target=data.get("dead_letter_target"),
            max_receive_count=int(data.get("max_receive_count", 3)),
            content_based_deduplication=bool(data.get("content_based_deduplication", False)),
            created_timestamp=float(data.get("created_timestamp", time.time())),
        )


@dataclass
class Message:
    queue_name: str
    body: str
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    md5_of_body: str = ""
    message_attributes: Dict[str, Any] = field(default_factory=dict)
    sent_timestamp: float = field(default_factory=time.time)
    visible_after: float = field(default_factory=time.time)
    receipt_handle: Optional[str] = None
    receive_count: int = 0
    first_received_timestamp: Optional[float] = None
    message_group_id: Optional[str] = None
    message_deduplication_id: Optional[str] = None
    status: str = "available"  # 'available', 'in_flight', 'deleted', 'dead_letter'
    dead_letter_reason: Optional[str] = None
    error_context: Optional[Dict[str, Any]] = None
    replayed_from_id: Optional[str] = None

    def __post_init__(self):
        if not self.md5_of_body:
            self.md5_of_body = hashlib.md5(self.body.encode("utf-8")).hexdigest()
        if not self.message_deduplication_id and self.queue_name.endswith(".fifo"):
            # Compute default hash if content-based deduplication is active
            self.message_deduplication_id = hashlib.sha256(self.body.encode("utf-8")).hexdigest()

    def generate_receipt_handle(self) -> str:
        self.receipt_handle = f"rh-{uuid.uuid4().hex[:16]}-{int(time.time()*1000)}"
        return self.receipt_handle

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_id": self.message_id,
            "queue_name": self.queue_name,
            "body": self.body,
            "md5_of_body": self.md5_of_body,
            "message_attributes": self.message_attributes,
            "sent_timestamp": self.sent_timestamp,
            "visible_after": self.visible_after,
            "receipt_handle": self.receipt_handle,
            "receive_count": self.receive_count,
            "first_received_timestamp": self.first_received_timestamp,
            "message_group_id": self.message_group_id,
            "message_deduplication_id": self.message_deduplication_id,
            "status": self.status,
            "dead_letter_reason": self.dead_letter_reason,
            "error_context": self.error_context,
            "replayed_from_id": self.replayed_from_id,
        }


@dataclass
class ReplayRequest:
    source_queue: str
    start_time: float
    end_time: float
    filter_expr: Optional[str] = None  # text regex or jsonpath
    target_queue: Optional[str] = None  # None => original queue
    dry_run: bool = False
    max_count: int = 1000


@dataclass
class DiagnosisReport:
    category: str
    description: str
    affected_count: int
    sample_message_ids: List[str]
    sample_payload: Dict[str, Any]
    detected_issue: str
    suggested_patch: Dict[str, Any]  # e.g., {"rename_keys": {"user": "user_id"}, "set_defaults": {"tier": "free"}}
    confidence: float

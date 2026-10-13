"""
ChronoQueue Deterministic Time-Travel Replay Engine
Enables replaying historical message traffic across any time window into live or shadow queues.
"""

from __future__ import annotations
import json
import re
import time
from typing import Any, Callable, Dict, List, Optional
from .models import ReplayRequest
from .storage import Storage
from .engine import QueueEngine


class TimeTravelReplayEngine:
    def __init__(self, storage: Storage, engine: QueueEngine):
        self.storage = storage
        self.engine = engine

    def execute_replay(self, req: ReplayRequest) -> Dict[str, Any]:
        """
        Executes a deterministic replay of messages sent between req.start_time and req.end_time.
        """
        raw_events = self.storage.query_chronolog(
            queue_name=req.source_queue,
            start_time=req.start_time,
            end_time=req.end_time,
            limit=req.max_count
        )

        filter_fn = self._build_filter(req.filter_expr)
        matched_records = []

        for record in raw_events:
            if filter_fn(record):
                matched_records.append(record)

        target_queue = req.target_queue or req.source_queue

        if req.dry_run:
            return {
                "dry_run": True,
                "source_queue": req.source_queue,
                "target_queue": target_queue,
                "time_window": {
                    "start": req.start_time,
                    "end": req.end_time,
                    "duration_seconds": round(req.end_time - req.start_time, 2)
                },
                "total_scanned": len(raw_events),
                "matched_count": len(matched_records),
                "replayed_count": 0,
                "samples": [
                    {
                        "original_id": r["message_id"],
                        "timestamp": r["timestamp"],
                        "body_preview": r["body"][:120] + ("..." if len(r["body"]) > 120 else "")
                    }
                    for r in matched_records[:5]
                ]
            }

        # Actual execution
        replayed_messages = []
        for record in matched_records:
            msg = self.engine.send_message(
                queue_name=target_queue,
                body=record["body"],
                message_attributes=record["message_attributes"],
                replayed_from_id=record["message_id"]
            )
            replayed_messages.append(msg.message_id)

        return {
            "dry_run": False,
            "source_queue": req.source_queue,
            "target_queue": target_queue,
            "total_scanned": len(raw_events),
            "matched_count": len(matched_records),
            "replayed_count": len(replayed_messages),
            "replayed_ids": replayed_messages[:20],
            "completed_at": time.time()
        }

    def _build_filter(self, filter_expr: Optional[str]) -> Callable[[Dict[str, Any]], bool]:
        if not filter_expr or not filter_expr.strip():
            return lambda _: True

        expr = filter_expr.strip()

        # Check if filter is a key=value pair (e.g., status=FAILED or type=checkout)
        if "=" in expr and not expr.startswith("{"):
            key, val = [p.strip() for p in expr.split("=", 1)]
            val = val.strip("'\"")

            def kv_filter(record: Dict[str, Any]) -> bool:
                # First check attributes
                attrs = record.get("message_attributes", {})
                if key in attrs and str(attrs[key].get("StringValue", attrs[key])) == val:
                    return True
                # Check JSON body
                try:
                    data = json.loads(record["body"])
                    if isinstance(data, dict) and str(data.get(key)) == val:
                        return True
                except Exception:
                    pass
                return False

            return kv_filter

        # Fallback to regex pattern matching on body
        try:
            pattern = re.compile(expr, re.IGNORECASE)
            return lambda record: bool(pattern.search(record["body"]))
        except re.error:
            # Simple substring match
            return lambda record: expr.lower() in record["body"].lower()

"""
ChronoQueue Python Client SDK
Zero-dependency client library using standard library urllib.
"""

from __future__ import annotations
import json
import time
import urllib.request
import urllib.parse
from typing import Any, Dict, List, Optional


class ChronoClient:
    def __init__(self, endpoint_url: str = "http://localhost:9324", engine: Optional[Any] = None):
        self.endpoint_url = endpoint_url.rstrip("/")
        self.engine = engine
        self._replay_engine = None
        self._dlq_engine = None
        if self.engine:
            from .replay import TimeTravelReplayEngine
            from .self_healing import SelfHealingDLQEngine
            self._replay_engine = TimeTravelReplayEngine(self.engine.storage, self.engine)
            self._dlq_engine = SelfHealingDLQEngine(self.engine.storage, self.engine)

    def _request(self, method: str, path: str, data: Optional[Dict[str, Any]] = None, params: Optional[Dict[str, Any]] = None) -> Any:
        if self.engine:
            return self._dispatch_embedded(method, path, data, params)

        url = f"{self.endpoint_url}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)

        req = urllib.request.Request(url, method=method)
        req.add_header("Content-Type", "application/json")

        payload_bytes = json.dumps(data).encode("utf-8") if data is not None else None
        with urllib.request.urlopen(req, data=payload_bytes) as resp:
            content = resp.read().decode("utf-8")
            if content:
                return json.loads(content)
            return None

    def _dispatch_embedded(self, method: str, path: str, data: Optional[Dict[str, Any]], params: Optional[Dict[str, Any]]) -> Any:
        params = params or {}
        data = data or {}
        if path == "/api/queues" and method == "POST":
            q = self.engine.create_queue(data["name"], data)
            return q.to_dict()
        if path == "/api/queues" and method == "GET":
            return [q.to_dict() for q in self.engine.list_queues()]
        if path == "/api/messages/send" and method == "POST":
            m = self.engine.send_message(
                queue_name=data["queue_name"],
                body=data["body"],
                delay_seconds=data.get("delay_seconds"),
                message_attributes=data.get("message_attributes"),
                message_group_id=data.get("message_group_id"),
                message_deduplication_id=data.get("message_deduplication_id")
            )
            return m.to_dict()
        if path == "/api/messages/receive" and method == "GET":
            msgs = self.engine.receive_messages(
                queue_name=params["queue"],
                max_number_of_messages=int(params.get("max", 1)),
                visibility_timeout=int(params.get("visibility", 30)),
                wait_time_seconds=int(params.get("wait", 0))
            )
            return [m.to_dict() for m in msgs]
        if path == "/api/messages/delete" and method == "POST":
            ok = self.engine.delete_message(data["queue_name"], data["receipt_handle"])
            return {"success": ok}
        if path == "/api/messages/failure" and method == "POST":
            ok = self.engine.report_failure(data["queue_name"], data["receipt_handle"], data["error"], data.get("stack_trace"))
            return {"reported": ok}
        if path == "/api/chrono/replay" and method == "POST":
            from .models import ReplayRequest
            req = ReplayRequest(
                source_queue=data["source_queue"],
                start_time=float(data.get("start_time", 0)),
                end_time=float(data.get("end_time", time.time())),
                filter_expr=data.get("filter_expr"),
                target_queue=data.get("target_queue") or None,
                dry_run=bool(data.get("dry_run", False))
            )
            return self._replay_engine.execute_replay(req)
        if path == "/api/chrono/dlq/diagnose" and method == "GET":
            return self._dlq_engine.diagnose_dlq(params["queue"])
        if path == "/api/chrono/dlq/redrive" and method == "POST":
            return self._dlq_engine.redrive_with_patch(data["source_dlq"], data["target_queue"], data.get("patch_recipe", {}))
        return None

    def create_queue(self, name: str, dead_letter_target: Optional[str] = None, max_receive_count: int = 3, is_fifo: bool = False) -> Dict[str, Any]:
        body = {
            "name": name,
            "FifoQueue": is_fifo,
            "DeadLetterTarget": dead_letter_target,
            "MaxReceiveCount": max_receive_count
        }
        return self._request("POST", "/api/queues", data=body)

    def list_queues(self) -> List[Dict[str, Any]]:
        return self._request("GET", "/api/queues")

    def send_message(
        self,
        queue_name: str,
        body: Any,
        message_group_id: Optional[str] = None,
        message_deduplication_id: Optional[str] = None,
        delay_seconds: int = 0
    ) -> Dict[str, Any]:
        msg_str = json.dumps(body) if not isinstance(body, str) else body
        payload = {
            "queue_name": queue_name,
            "body": msg_str,
            "message_group_id": message_group_id,
            "message_deduplication_id": message_deduplication_id,
            "delay_seconds": delay_seconds
        }
        return self._request("POST", "/api/messages/send", data=payload)

    def receive_messages(self, queue_name: str, max_messages: int = 1, wait_time: int = 0, visibility: int = 30) -> List[Dict[str, Any]]:
        params = {
            "queue": queue_name,
            "max": max_messages,
            "wait": wait_time,
            "visibility": visibility
        }
        return self._request("GET", "/api/messages/receive", params=params)

    def delete_message(self, queue_name: str, receipt_handle: str) -> bool:
        res = self._request("POST", "/api/messages/delete", data={"queue_name": queue_name, "receipt_handle": receipt_handle})
        return res.get("success", False)

    def report_failure(self, queue_name: str, receipt_handle: str, error: str, stack_trace: Optional[str] = None) -> bool:
        res = self._request("POST", "/api/messages/failure", data={
            "queue_name": queue_name,
            "receipt_handle": receipt_handle,
            "error": error,
            "stack_trace": stack_trace
        })
        return res.get("reported", False)

    def replay(
        self,
        source_queue: str,
        start_time: float,
        end_time: float,
        filter_expr: Optional[str] = None,
        target_queue: Optional[str] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        payload = {
            "source_queue": source_queue,
            "start_time": start_time,
            "end_time": end_time,
            "filter_expr": filter_expr,
            "target_queue": target_queue,
            "dry_run": dry_run
        }
        return self._request("POST", "/api/chrono/replay", data=payload)

    def diagnose_dlq(self, dlq_name: str) -> List[Dict[str, Any]]:
        return self._request("GET", "/api/chrono/dlq/diagnose", params={"queue": dlq_name})

    def redrive(self, source_dlq: str, target_queue: str, patch_recipe: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/api/chrono/dlq/redrive", data={
            "source_dlq": source_dlq,
            "target_queue": target_queue,
            "patch_recipe": patch_recipe
        })

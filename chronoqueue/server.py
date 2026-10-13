"""
ChronoQueue Server
Provides an AWS SQS compatible interface, REST APIs, and serves the Web Dashboard.
Zero external dependencies; uses Python standard library http.server & socketserver.
"""

from __future__ import annotations
import json
import logging
import os
import pathlib
import re
import socketserver
import time
import urllib.parse
from http.server import SimpleHTTPRequestHandler
from typing import Any, Dict, Optional

from .engine import QueueEngine
from .models import ReplayRequest
from .replay import TimeTravelReplayEngine
from .self_healing import SelfHealingDLQEngine
from .storage import Storage

logger = logging.getLogger("chronoqueue.server")


class ThreadedHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


class ChronoQueueHandler(SimpleHTTPRequestHandler):
    engine: QueueEngine
    replay_engine: TimeTravelReplayEngine
    dlq_engine: SelfHealingDLQEngine
    web_dir: str

    def log_message(self, format, *args):
        # Quiet default logger to reduce terminal spam
        pass

    def send_json(self, status: int, data: Any):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path == "/" or path == "/index.html":
            html_file = os.path.join(self.web_dir, "index.html")
            if os.path.exists(html_file):
                with open(html_file, "rb") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return

        if path == "/api/metrics":
            queues = self.engine.list_queues()
            avail = 0
            flight = 0
            dlq_cnt = 0
            for q in queues:
                m = self.engine.storage.get_queue_metrics(q.name)
                avail += m["approximate_number_of_messages"]
                flight += m["approximate_number_of_messages_not_visible"]
                dlq_cnt += m["dead_letter_count"]
            self.send_json(200, {
                "total_queues": len(queues),
                "total_available": avail,
                "total_in_flight": flight,
                "total_dead_letter": dlq_cnt
            })
            return

        if path == "/api/queues":
            queues = self.engine.list_queues()
            resp = []
            for q in queues:
                q_dict = q.to_dict()
                q_dict["metrics"] = self.engine.storage.get_queue_metrics(q.name)
                resp.append(q_dict)
            self.send_json(200, resp)
            return

        if path == "/api/chrono/dlq/diagnose":
            q_name = query.get("queue", [""])[0]
            if not q_name:
                self.send_json(400, {"error": "Missing queue query param"})
                return
            reports = self.dlq_engine.diagnose_dlq(q_name)
            self.send_json(200, reports)
            return

        if path == "/api/messages/receive":
            q_name = query.get("queue", [""])[0]
            max_msgs = int(query.get("max", ["1"])[0])
            wait_time = int(query.get("wait", ["0"])[0])
            vis_timeout = int(query.get("visibility", ["30"])[0])
            if not q_name:
                self.send_json(400, {"error": "queue query param required"})
                return
            msgs = self.engine.receive_messages(q_name, max_number_of_messages=max_msgs, visibility_timeout=vis_timeout, wait_time_seconds=wait_time)
            self.send_json(200, [m.to_dict() for m in msgs])
            return

        self.send_json(404, {"error": "Endpoint not found"})

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
        try:
            body = json.loads(raw_body)
        except Exception:
            body = {}

        # Standard SQS Action simulation fallback
        action = self.headers.get("X-Amz-Target") or body.get("Action")
        if action == "AmazonSQS.SendMessage" or action == "SendMessage":
            return self._handle_sqs_send(body)
        if action == "AmazonSQS.ReceiveMessage" or action == "ReceiveMessage":
            return self._handle_sqs_receive(body)

        if path == "/api/queues":
            name = body.get("name")
            if not name:
                self.send_json(400, {"error": "Queue name required"})
                return
            q = self.engine.create_queue(name, body)
            self.send_json(201, q.to_dict())
            return

        if path.startswith("/api/queues/") and path.endswith("/purge"):
            name = path.split("/")[3]
            self.engine.purge_queue(name)
            self.send_json(200, {"status": "purged", "queue": name})
            return

        if path == "/api/messages/send":
            q_name = body.get("queue_name")
            msg_body = body.get("body")
            if not q_name or not msg_body:
                self.send_json(400, {"error": "queue_name and body required"})
                return
            msg = self.engine.send_message(
                queue_name=q_name,
                body=msg_body,
                delay_seconds=body.get("delay_seconds"),
                message_attributes=body.get("message_attributes"),
                message_group_id=body.get("message_group_id"),
                message_deduplication_id=body.get("message_deduplication_id")
            )
            self.send_json(200, msg.to_dict())
            return

        if path == "/api/messages/delete":
            q_name = body.get("queue_name")
            receipt = body.get("receipt_handle")
            ok = self.engine.delete_message(q_name, receipt)
            self.send_json(200, {"success": ok})
            return

        if path == "/api/messages/failure":
            q_name = body.get("queue_name")
            receipt = body.get("receipt_handle")
            err_msg = body.get("error", "Processing error")
            stack = body.get("stack_trace")
            ok = self.engine.report_failure(q_name, receipt, err_msg, stack)
            self.send_json(200, {"reported": ok})
            return

        if path == "/api/chrono/replay":
            req = ReplayRequest(
                source_queue=body["source_queue"],
                start_time=float(body.get("start_time", 0)),
                end_time=float(body.get("end_time", time.time())),
                filter_expr=body.get("filter_expr"),
                target_queue=body.get("target_queue") or None,
                dry_run=bool(body.get("dry_run", False))
            )
            result = self.replay_engine.execute_replay(req)
            self.send_json(200, result)
            return

        if path == "/api/chrono/dlq/dryrun-patch":
            dlq = body.get("queue_name")
            recipe = body.get("patch_recipe", {})
            previews = self.dlq_engine.dry_run_patch(dlq, recipe)
            self.send_json(200, previews)
            return

        if path == "/api/chrono/dlq/redrive":
            dlq = body.get("source_dlq")
            target = body.get("target_queue")
            recipe = body.get("patch_recipe", {})
            result = self.dlq_engine.redrive_with_patch(dlq, target, recipe)
            self.send_json(200, result)
            return

        if path.startswith("/api/simulate/"):
            scenario = path.split("/")[-1]
            return self._handle_simulation(scenario)

        self.send_json(404, {"error": "Not Found"})

    def _handle_sqs_send(self, body: Dict[str, Any]):
        q_url = body.get("QueueUrl", "")
        q_name = q_url.split("/")[-1] if "/" in q_url else body.get("QueueName", "default")
        msg = self.engine.send_message(
            queue_name=q_name,
            body=body.get("MessageBody", ""),
            delay_seconds=body.get("DelaySeconds"),
            message_attributes=body.get("MessageAttributes"),
            message_group_id=body.get("MessageGroupId"),
            message_deduplication_id=body.get("MessageDeduplicationId")
        )
        self.send_json(200, {
            "SendMessageResponse": {
                "SendMessageResult": {
                    "MD5OfMessageBody": msg.md5_of_body,
                    "MessageId": msg.message_id
                }
            }
        })

    def _handle_sqs_receive(self, body: Dict[str, Any]):
        q_url = body.get("QueueUrl", "")
        q_name = q_url.split("/")[-1] if "/" in q_url else body.get("QueueName", "default")
        msgs = self.engine.receive_messages(
            queue_name=q_name,
            max_number_of_messages=int(body.get("MaxNumberOfMessages", 1)),
            visibility_timeout=int(body.get("VisibilityTimeout", 30))
        )
        self.send_json(200, {
            "ReceiveMessageResponse": {
                "ReceiveMessageResult": {
                    "Messages": [
                        {
                            "MessageId": m.message_id,
                            "ReceiptHandle": m.receipt_handle,
                            "MD5OfBody": m.md5_of_body,
                            "Body": m.body
                        }
                        for m in msgs
                    ]
                }
            }
        })

    def _handle_simulation(self, scenario: str):
        if scenario == "burst":
            q_name = "orders-pipeline"
            self.engine.create_queue(q_name)
            ids = []
            for i in range(15):
                m = self.engine.send_message(
                    queue_name=q_name,
                    body=json.dumps({"order_id": f"ORD-{1000+i}", "amount": 19.99 + i, "status": "pending"})
                )
                ids.append(m.message_id)
            self.send_json(200, {"scenario": "burst", "queue": q_name, "sent_count": len(ids)})
            return

        if scenario == "poison":
            q_name = "payments-stream"
            dlq_name = "payments-stream-dlq"
            self.engine.create_queue(q_name, attributes={"RedrivePolicy": {"deadLetterTargetArn": dlq_name, "maxReceiveCount": 2}})
            self.engine.create_queue(dlq_name)

            # Inject 5 messages with schema naming drift: "customerId" instead of "customer_id"
            for i in range(5):
                msg = self.engine.send_message(
                    queue_name=q_name,
                    body=json.dumps({"tx_id": f"TX-{200+i}", "customerId": f"USER-{50+i}", "amount": 100})
                )
                # Simulate consumer attempting to read, failing with KeyError, and routing to DLQ
                rec = self.engine.receive_messages(q_name, max_number_of_messages=1)[0]
                self.engine.report_failure(
                    q_name,
                    rec.receipt_handle,
                    error_msg="KeyError: 'customer_id'",
                    stack_trace="Traceback (most recent call last):\n  File 'worker.py', line 24\n    uid = payload['customer_id']\nKeyError: 'customer_id'"
                )
                rec2 = self.engine.receive_messages(q_name, max_number_of_messages=1)[0]
                self.engine.report_failure(
                    q_name,
                    rec2.receipt_handle,
                    error_msg="KeyError: 'customer_id'",
                    stack_trace="Traceback (most recent call last):\n  File 'worker.py', line 24\n    uid = payload['customer_id']\nKeyError: 'customer_id'"
                )

            self.send_json(200, {"scenario": "poison", "status": "Injected 5 messages that failed and landed in " + dlq_name})
            return

        if scenario == "fifo":
            fifo_name = "ledger.fifo"
            self.engine.create_queue(fifo_name, attributes={"FifoQueue": True})
            m1 = self.engine.send_message(fifo_name, json.dumps({"entry": 1}), message_group_id="g1", message_deduplication_id="dedup-token-1")
            m2 = self.engine.send_message(fifo_name, json.dumps({"entry": 1, "retry": True}), message_group_id="g1", message_deduplication_id="dedup-token-1")
            self.send_json(200, {
                "scenario": "fifo",
                "queue": fifo_name,
                "first_id": m1.message_id,
                "second_id": m2.message_id,
                "deduplicated_identical": m1.message_id == m2.message_id
            })
            return

        self.send_json(400, {"error": "Unknown scenario"})


def seed_sample_data(engine: QueueEngine):
    if len(engine.list_queues()) > 0:
        return

    # 1. Primary orders queue with DLQ configured
    dlq_name = "orders-dlq"
    primary_name = "orders-primary"
    engine.create_queue(dlq_name)
    engine.create_queue(primary_name, attributes={
        "RedrivePolicy": {"deadLetterTargetArn": dlq_name, "maxReceiveCount": 2}
    })

    for i in range(8):
        engine.send_message(
            primary_name,
            json.dumps({"order_id": f"ORD-100{i}", "customer_id": f"CUST-50{i}", "amount": round(25.50 + i * 12.3, 2), "status": "pending"})
        )

    # 2. Poison pills in orders-dlq
    for i in range(4):
        msg = engine.send_message(
            dlq_name,
            json.dumps({"order_id": f"ORD-ERR-90{i}", "customerId": f"USER-77{i}", "total": 99.00 + i}),
        )
        msg.status = "dead_letter"
        msg.dead_letter_reason = "KeyError: 'customer_id'"
        msg.error_context = {
            "error": "KeyError: 'customer_id'",
            "stack_trace": "File 'worker.py', line 34, in process\n    cid = payload['customer_id']\nKeyError: 'customer_id'",
            "failed_at": time.time() - (60 * (i + 1))
        }
        engine.storage.update_message(msg)

    # 3. FIFO settlements queue
    fifo_name = "settlements.fifo"
    engine.create_queue(fifo_name, attributes={"FifoQueue": True})
    for i in range(5):
        engine.send_message(
            fifo_name,
            json.dumps({"tx_id": f"TX-400{i}", "batch": "settle-daily", "amount": 150 * (i + 1)}),
            message_group_id="ledger-group-1",
            message_deduplication_id=f"dedup-seed-{i}"
        )


def run_server(port: int = 9324, db_path: str = "chronoqueue.db"):
    storage = Storage(db_path=db_path)
    engine = QueueEngine(storage=storage)
    replay_engine = TimeTravelReplayEngine(storage=storage, engine=engine)
    dlq_engine = SelfHealingDLQEngine(storage=storage, engine=engine)

    # Automatically populate realistic sample data on first start
    seed_sample_data(engine)

    web_dir = os.path.join(pathlib.Path(__file__).parent, "web")

    ChronoQueueHandler.engine = engine
    ChronoQueueHandler.replay_engine = replay_engine
    ChronoQueueHandler.dlq_engine = dlq_engine
    ChronoQueueHandler.web_dir = web_dir

    server = ThreadedHTTPServer(("0.0.0.0", port), ChronoQueueHandler)
    print(f"🚀 ChronoQueue Server started at http://localhost:{port}")
    print(f"📁 Web Dashboard available at http://localhost:{port}/")
    print(f"💾 Persistent storage: {db_path}")
    return server

"""
ChronoQueue Storage Layer
Embedded SQLite storage with an immutable ChronoLog for deterministic time-travel and auditing.
"""

from __future__ import annotations
import json
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional
from .models import Message, QueueConfig


class Storage:
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        with self._lock, self._conn:
            self._conn.executescript("""
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;

            CREATE TABLE IF NOT EXISTS queues (
                name TEXT PRIMARY KEY,
                config_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS messages (
                message_id TEXT PRIMARY KEY,
                queue_name TEXT NOT NULL,
                body TEXT NOT NULL,
                md5_of_body TEXT NOT NULL,
                message_attributes TEXT NOT NULL,
                sent_timestamp REAL NOT NULL,
                visible_after REAL NOT NULL,
                receipt_handle TEXT,
                receive_count INTEGER DEFAULT 0,
                first_received_timestamp REAL,
                message_group_id TEXT,
                message_deduplication_id TEXT,
                status TEXT NOT NULL, -- 'available', 'in_flight', 'deleted', 'dead_letter'
                dead_letter_reason TEXT,
                error_context TEXT,
                replayed_from_id TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_messages_queue_status_vis 
                ON messages(queue_name, status, visible_after);

            CREATE INDEX IF NOT EXISTS idx_messages_receipt_handle 
                ON messages(receipt_handle);

            CREATE INDEX IF NOT EXISTS idx_messages_dedup 
                ON messages(queue_name, message_deduplication_id);

            -- Immutable ChronoLog for Time-Travel Replay & Auditing
            CREATE TABLE IF NOT EXISTS chronolog (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL,
                queue_name TEXT NOT NULL,
                event_type TEXT NOT NULL, -- 'sent', 'received', 'deleted', 'dlq', 'visibility_extended'
                timestamp REAL NOT NULL,
                body TEXT NOT NULL,
                message_attributes TEXT NOT NULL,
                metadata TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_chronolog_lookup 
                ON chronolog(queue_name, timestamp);
            """)

    def save_queue(self, config: QueueConfig):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO queues (name, config_json) VALUES (?, ?)",
                (config.name, json.dumps(config.to_dict()))
            )

    def get_queue(self, name: str) -> Optional[QueueConfig]:
        with self._lock:
            cur = self._conn.execute("SELECT config_json FROM queues WHERE name = ?", (name,))
            row = cur.fetchone()
            if not row:
                return None
            return QueueConfig.from_dict(json.loads(row["config_json"]))

    def list_queues(self) -> List[QueueConfig]:
        with self._lock:
            cur = self._conn.execute("SELECT config_json FROM queues ORDER BY name ASC")
            return [QueueConfig.from_dict(json.loads(r["config_json"])) for r in cur.fetchall()]

    def delete_queue(self, name: str) -> bool:
        with self._lock, self._conn:
            cur = self._conn.execute("DELETE FROM queues WHERE name = ?", (name,))
            if cur.rowcount > 0:
                self._conn.execute("DELETE FROM messages WHERE queue_name = ?", (name,))
                return True
            return False

    def insert_message(self, msg: Message):
        with self._lock, self._conn:
            self._conn.execute("""
                INSERT OR REPLACE INTO messages (
                    message_id, queue_name, body, md5_of_body, message_attributes,
                    sent_timestamp, visible_after, receipt_handle, receive_count,
                    first_received_timestamp, message_group_id, message_deduplication_id,
                    status, dead_letter_reason, error_context, replayed_from_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                msg.message_id, msg.queue_name, msg.body, msg.md5_of_body,
                json.dumps(msg.message_attributes), msg.sent_timestamp, msg.visible_after,
                msg.receipt_handle, msg.receive_count, msg.first_received_timestamp,
                msg.message_group_id, msg.message_deduplication_id, msg.status,
                msg.dead_letter_reason,
                json.dumps(msg.error_context) if msg.error_context else None,
                msg.replayed_from_id
            ))
            # Append to immutable ChronoLog
            self._append_chronolog(msg.message_id, msg.queue_name, "sent", msg.sent_timestamp, msg.body, msg.message_attributes)

    def _append_chronolog(self, message_id: str, queue_name: str, event_type: str, timestamp: float, body: str, attributes: dict, metadata: Optional[dict] = None):
        self._conn.execute("""
            INSERT INTO chronolog (message_id, queue_name, event_type, timestamp, body, message_attributes, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            message_id, queue_name, event_type, timestamp, body,
            json.dumps(attributes), json.dumps(metadata or {})
        ))

    def update_message(self, msg: Message):
        with self._lock, self._conn:
            self._conn.execute("""
                UPDATE messages SET
                    visible_after = ?, receipt_handle = ?, receive_count = ?,
                    first_received_timestamp = ?, status = ?, dead_letter_reason = ?,
                    error_context = ?
                WHERE message_id = ?
            """, (
                msg.visible_after, msg.receipt_handle, msg.receive_count,
                msg.first_received_timestamp, msg.status, msg.dead_letter_reason,
                json.dumps(msg.error_context) if msg.error_context else None,
                msg.message_id
            ))

    def get_message(self, message_id: str) -> Optional[Message]:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM messages WHERE message_id = ?", (message_id,))
            row = cur.fetchone()
            return self._row_to_message(row) if row else None

    def get_message_by_receipt(self, receipt_handle: str) -> Optional[Message]:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM messages WHERE receipt_handle = ?", (receipt_handle,))
            row = cur.fetchone()
            return self._row_to_message(row) if row else None

    def find_dedup_message(self, queue_name: str, dedup_id: str, window_seconds: float = 300) -> Optional[Message]:
        now = time.time()
        with self._lock:
            cur = self._conn.execute("""
                SELECT * FROM messages 
                WHERE queue_name = ? AND message_deduplication_id = ? AND (? - sent_timestamp) <= ?
            """, (queue_name, dedup_id, now, window_seconds))
            row = cur.fetchone()
            return self._row_to_message(row) if row else None

    def fetch_available_messages(self, queue_name: str, limit: int, now: float) -> List[Message]:
        with self._lock:
            # First reset any in_flight messages whose visible_after expired
            self._conn.execute("""
                UPDATE messages SET status = 'available', receipt_handle = NULL
                WHERE queue_name = ? AND status = 'in_flight' AND visible_after <= ?
            """, (queue_name, now))

            cur = self._conn.execute("""
                SELECT * FROM messages 
                WHERE queue_name = ? AND status = 'available' AND visible_after <= ?
                ORDER BY sent_timestamp ASC LIMIT ?
            """, (queue_name, now, limit))
            return [self._row_to_message(r) for r in cur.fetchall()]

    def list_queue_messages(self, queue_name: str, status: Optional[str] = None, limit: int = 100) -> List[Message]:
        with self._lock:
            if status:
                cur = self._conn.execute(
                    "SELECT * FROM messages WHERE queue_name = ? AND status = ? ORDER BY sent_timestamp DESC LIMIT ?",
                    (queue_name, status, limit)
                )
            else:
                cur = self._conn.execute(
                    "SELECT * FROM messages WHERE queue_name = ? ORDER BY sent_timestamp DESC LIMIT ?",
                    (queue_name, limit)
                )
            return [self._row_to_message(r) for r in cur.fetchall()]

    def get_queue_metrics(self, queue_name: str) -> Dict[str, int]:
        now = time.time()
        with self._lock:
            cur = self._conn.execute("""
                SELECT 
                    SUM(CASE WHEN status = 'available' AND visible_after <= ? THEN 1 ELSE 0 END) as available,
                    SUM(CASE WHEN (status = 'in_flight' AND visible_after > ?) OR (status = 'available' AND visible_after > ?) THEN 1 ELSE 0 END) as in_flight,
                    SUM(CASE WHEN status = 'dead_letter' THEN 1 ELSE 0 END) as dead_letter,
                    SUM(CASE WHEN status = 'deleted' THEN 1 ELSE 0 END) as deleted_total
                FROM messages WHERE queue_name = ?
            """, (now, now, now, queue_name))
            row = cur.fetchone()
            return {
                "approximate_number_of_messages": row["available"] or 0,
                "approximate_number_of_messages_not_visible": row["in_flight"] or 0,
                "approximate_number_of_messages_delayed": 0,
                "dead_letter_count": row["dead_letter"] or 0,
                "deleted_count": row["deleted_total"] or 0
            }

    def purge_queue(self, queue_name: str):
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM messages WHERE queue_name = ?", (queue_name,))

    def query_chronolog(self, queue_name: str, start_time: float, end_time: float, limit: int = 1000) -> List[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute("""
                SELECT * FROM chronolog
                WHERE queue_name = ? AND event_type = 'sent' AND timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp ASC LIMIT ?
            """, (queue_name, start_time, end_time, limit))
            results = []
            for r in cur.fetchall():
                results.append({
                    "log_id": r["log_id"],
                    "message_id": r["message_id"],
                    "queue_name": r["queue_name"],
                    "timestamp": r["timestamp"],
                    "body": r["body"],
                    "message_attributes": json.loads(r["message_attributes"]),
                    "metadata": json.loads(r["metadata"] or "{}")
                })
            return results

    def close(self):
        with self._lock:
            self._conn.close()

    def _row_to_message(self, row: sqlite3.Row) -> Message:
        return Message(
            message_id=row["message_id"],
            queue_name=row["queue_name"],
            body=row["body"],
            md5_of_body=row["md5_of_body"],
            message_attributes=json.loads(row["message_attributes"] or "{}"),
            sent_timestamp=row["sent_timestamp"],
            visible_after=row["visible_after"],
            receipt_handle=row["receipt_handle"],
            receive_count=row["receive_count"],
            first_received_timestamp=row["first_received_timestamp"],
            message_group_id=row["message_group_id"],
            message_deduplication_id=row["message_deduplication_id"],
            status=row["status"],
            dead_letter_reason=row["dead_letter_reason"],
            error_context=json.loads(row["error_context"]) if row["error_context"] else None,
            replayed_from_id=row["replayed_from_id"]
        )

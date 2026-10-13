"""
Tests for Deterministic Time-Travel Replay Engine
"""

import time
import unittest
from chronoqueue.storage import Storage
from chronoqueue.engine import QueueEngine
from chronoqueue.replay import TimeTravelReplayEngine
from chronoqueue.models import ReplayRequest


class TestReplayEngine(unittest.TestCase):
    def setUp(self):
        self.storage = Storage(":memory:")
        self.engine = QueueEngine(self.storage)
        self.replay = TimeTravelReplayEngine(self.storage, self.engine)

    def tearDown(self):
        self.storage.close()

    def test_time_travel_replay_with_filter(self):
        self.engine.create_queue("notifications")
        t0 = time.time()

        # Send varying messages
        self.engine.send_message("notifications", '{"type": "email", "recipient": "alice@test.com"}')
        self.engine.send_message("notifications", '{"type": "sms", "recipient": "+123456789"}')
        self.engine.send_message("notifications", '{"type": "email", "recipient": "bob@test.com"}')

        t1 = time.time() + 1

        # Dry-run replay with JSON filter type=email
        dry_req = ReplayRequest(
            source_queue="notifications",
            start_time=t0,
            end_time=t1,
            filter_expr="type=email",
            dry_run=True
        )
        dry_res = self.replay.execute_replay(dry_req)
        self.assertTrue(dry_res["dry_run"])
        self.assertEqual(dry_res["total_scanned"], 3)
        self.assertEqual(dry_res["matched_count"], 2)
        self.assertEqual(dry_res["replayed_count"], 0)

        # Live replay to shadow queue
        shadow_req = ReplayRequest(
            source_queue="notifications",
            target_queue="notifications-debug-shadow",
            start_time=t0,
            end_time=t1,
            filter_expr="type=email",
            dry_run=False
        )
        live_res = self.replay.execute_replay(shadow_req)
        self.assertEqual(live_res["replayed_count"], 2)

        # Inspect target shadow queue
        shadow_msgs = self.engine.receive_messages("notifications-debug-shadow", max_number_of_messages=10)
        self.assertEqual(len(shadow_msgs), 2)
        for m in shadow_msgs:
            self.assertTrue("alice@test.com" in m.body or "bob@test.com" in m.body)
            self.assertIsNotNone(m.replayed_from_id)


if __name__ == "__main__":
    unittest.main()

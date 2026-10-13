"""
Tests for Self-Healing DLQ, Diagnosis, and Sandbox Auto-Patching
"""

import json
import unittest
from chronoqueue.storage import Storage
from chronoqueue.engine import QueueEngine
from chronoqueue.self_healing import SelfHealingDLQEngine


class TestSelfHealingDLQ(unittest.TestCase):
    def setUp(self):
        self.storage = Storage(":memory:")
        self.engine = QueueEngine(self.storage)
        self.healer = SelfHealingDLQEngine(self.storage, self.engine)

    def tearDown(self):
        self.storage.close()

    def test_diagnosis_and_dryrun_patch(self):
        dlq_name = "orders-dlq"
        self.engine.create_queue(dlq_name)

        # Insert 3 messages with naming drift: "customerId" instead of expected "customer_id"
        for i in range(3):
            msg = self.engine.send_message(
                dlq_name,
                json.dumps({"order_id": f"ORD-{i}", "customerId": f"USER-{i}", "total": 50})
            )
            # Attach error context simulating worker traceback
            msg.dead_letter_reason = "KeyError: 'customer_id'"
            msg.error_context = {
                "error": "KeyError: 'customer_id'",
                "stack_trace": "File worker.py line 12: payload['customer_id']\nKeyError: 'customer_id'"
            }
            self.storage.update_message(msg)

        # Run diagnosis
        reports = self.healer.diagnose_dlq(dlq_name)
        self.assertEqual(len(reports), 1)
        rep = reports[0]
        self.assertEqual(rep["category"], "SCHEMA_MISSING_KEY")
        self.assertIn("rename_keys", rep["suggested_patch"])
        self.assertEqual(rep["suggested_patch"]["rename_keys"]["customerId"], "customer_id")

        # Test Dry Run Patch Sandbox
        previews = self.healer.dry_run_patch(dlq_name, rep["suggested_patch"])
        self.assertEqual(len(previews), 3)
        for p in previews:
            self.assertTrue(p["success"])
            patched_data = json.loads(p["patched_body"])
            self.assertIn("customer_id", patched_data)
            self.assertNotIn("customerId", patched_data)

        # Execute Redrive to primary queue
        target_q = "orders-repaired"
        self.engine.create_queue(target_q)
        redrive_res = self.healer.redrive_with_patch(dlq_name, target_q, rep["suggested_patch"])
        self.assertEqual(redrive_res["successfully_redriven"], 3)

        # Verify target queue now has the repaired messages
        repaired_msgs = self.engine.receive_messages(target_q, max_number_of_messages=5)
        self.assertEqual(len(repaired_msgs), 3)
        for rm in repaired_msgs:
            obj = json.loads(rm.body)
            self.assertIn("customer_id", obj)


if __name__ == "__main__":
    unittest.main()

"""
Tests for Core Queue Engine and SQS Semantics
"""

import time
import unittest
from chronoqueue.storage import Storage
from chronoqueue.engine import QueueEngine


class TestQueueEngine(unittest.TestCase):
    def setUp(self):
        self.storage = Storage(":memory:")
        self.engine = QueueEngine(self.storage)

    def tearDown(self):
        self.storage.close()

    def test_create_and_list_queues(self):
        q1 = self.engine.create_queue("orders-queue")
        self.assertEqual(q1.name, "orders-queue")
        self.assertFalse(q1.is_fifo)

        q2 = self.engine.create_queue("events.fifo")
        self.assertTrue(q2.is_fifo)

        queues = self.engine.list_queues()
        self.assertEqual(len(queues), 2)

    def test_send_and_receive_message(self):
        self.engine.create_queue("tasks")
        sent = self.engine.send_message("tasks", "process_item_42")
        self.assertEqual(sent.body, "process_item_42")

        received = self.engine.receive_messages("tasks", max_number_of_messages=1, visibility_timeout=5)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].body, "process_item_42")
        self.assertIsNotNone(received[0].receipt_handle)

        # Message is now in-flight, so receiving again immediately should return nothing
        second_poll = self.engine.receive_messages("tasks", max_number_of_messages=1)
        self.assertEqual(len(second_poll), 0)

        # Delete message
        deleted = self.engine.delete_message("tasks", received[0].receipt_handle)
        self.assertTrue(deleted)

    def test_fifo_deduplication(self):
        self.engine.create_queue("payments.fifo")
        msg1 = self.engine.send_message(
            "payments.fifo",
            '{"tx": 101}',
            message_group_id="user-1",
            message_deduplication_id="token-abc"
        )
        msg2 = self.engine.send_message(
            "payments.fifo",
            '{"tx": 101, "retry": true}',
            message_group_id="user-1",
            message_deduplication_id="token-abc"
        )

        # Both calls should resolve to the exact same message ID
        self.assertEqual(msg1.message_id, msg2.message_id)

    def test_dlq_routing_on_max_receive(self):
        self.engine.create_queue("dlq-bucket")
        self.engine.create_queue("primary-queue", attributes={
            "RedrivePolicy": {
                "deadLetterTargetArn": "dlq-bucket",
                "maxReceiveCount": 2
            }
        })

        self.engine.send_message("primary-queue", "poison_pill")

        # Receive attempt 1
        m1 = self.engine.receive_messages("primary-queue", visibility_timeout=0)[0]
        self.assertEqual(m1.receive_count, 1)

        # Receive attempt 2
        m2 = self.engine.receive_messages("primary-queue", visibility_timeout=0)[0]
        self.assertEqual(m2.receive_count, 2)

        # Receive attempt 3 -> should trigger DLQ route
        m3 = self.engine.receive_messages("primary-queue")
        self.assertEqual(len(m3), 0)

        # Verify message is in DLQ
        dlq_msgs = self.engine.receive_messages("dlq-bucket")
        self.assertEqual(len(dlq_msgs), 1)
        self.assertEqual(dlq_msgs[0].body, "poison_pill")


if __name__ == "__main__":
    unittest.main()

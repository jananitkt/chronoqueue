"""
ChronoQueue Interactive Live Demo
Demonstrates all novel features:
1. Standard & FIFO SQS Semantics + Deduplication
2. Poison pill isolation & Dead-Letter Queue (DLQ) threshold routing
3. DLQ Root-Cause Diagnosis, Sandbox Dry-Run Patching, and Patched Redrive
4. Deterministic Time-Travel Replay into a Shadow Debug Queue
"""

import json
import os
import sys
import threading
import time

from chronoqueue.client import ChronoClient
from chronoqueue.storage import Storage
from chronoqueue.engine import QueueEngine
from chronoqueue.server import run_server


def print_banner(text: str):
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)


def main():
    port = 9324
    db_file = "demo_chronoqueue.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    storage = Storage(db_path=db_file)
    engine = QueueEngine(storage=storage)

    # Initialize client in direct embedded engine mode
    client = ChronoClient(engine=engine)
    print("✓ Initialized ChronoQueue Engine")

    # Start HTTP server & Web UI in background so user can inspect in browser
    server = None
    try:
        server = run_server(port=port, db_path=db_file)
        srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
        srv_thread.start()
    except Exception:
        # If binding socket fails (e.g. sandbox restriction), demo proceeds in embedded mode
        pass

    print_banner("1. INITIALIZING QUEUES")
    dlq_res = client.create_queue("orders-dlq")
    print(f"✓ Created Dead-Letter Queue: {dlq_res['name']}")

    primary_res = client.create_queue("orders-primary", dead_letter_target="orders-dlq", max_receive_count=2)
    print(f"✓ Created Primary Queue: {primary_res['name']} (Max Receives before DLQ: 2)")

    fifo_res = client.create_queue("settlements.fifo", is_fifo=True)
    print(f"✓ Created FIFO Queue: {fifo_res['name']}")

    # --- Showcase 1: FIFO Deduplication ---
    print_banner("2. FIFO DEDUPLICATION TEST")
    print("Publishing two identical messages with deduplication token 'tx-abc-123'...")
    m1 = client.send_message("settlements.fifo", {"amount": 500, "curr": "USD"}, message_group_id="g1", message_deduplication_id="tx-abc-123")
    m2 = client.send_message("settlements.fifo", {"amount": 500, "curr": "USD", "retry": True}, message_group_id="g1", message_deduplication_id="tx-abc-123")
    print(f"First Message ID : {m1['message_id']}")
    print(f"Second Message ID: {m2['message_id']}")
    assert m1['message_id'] == m2['message_id'], "FIFO Deduplication failed!"
    print("✓ Result: Identical message ID returned; duplicate was cleanly suppressed!")

    # --- Showcase 2: Poison Pills & DLQ Routing ---
    print_banner("3. POISON PILLS & DLQ ROUTING")
    print("Publishing 3 messages with schema drift ('customerId' instead of 'customer_id')...")
    for i in range(3):
        client.send_message("orders-primary", {
            "order_id": f"ORD-202{i}",
            "customerId": f"CUST-99{i}",
            "total": 99.0 + i
        })

    print("Worker consuming messages and encountering KeyError...")
    for attempt in range(2):
        print(f"  Attempt {attempt + 1}:")
        msgs = client.receive_messages("orders-primary", max_messages=3, visibility=0)
        for m in msgs:
            print(f"    - Worker failed on msg {m['message_id'][:8]}... Reporting failure.")
            client.report_failure(
                "orders-primary",
                m["receipt_handle"],
                error="KeyError: 'customer_id'",
                stack_trace="Traceback (most recent call last):\n  File 'order_processor.py', line 45\n    cid = record['customer_id']\nKeyError: 'customer_id'"
            )

    # Next receive will trigger transfer to DLQ
    leftover = client.receive_messages("orders-primary", max_messages=3)
    print(f"✓ Messages remaining in primary queue: {len(leftover)}")

    dlq_items = client.receive_messages("orders-dlq", max_messages=5, visibility=0)
    print(f"✓ Messages quarantined in DLQ: {len(dlq_items)}")

    # --- Showcase 3: DLQ Diagnosis & Self-Healing ---
    print_banner("4. SELF-HEALING DLQ DIAGNOSIS & AUTO-PATCH")
    print("Running automated root-cause analysis on 'orders-dlq'...")
    reports = client.diagnose_dlq("orders-dlq")
    print(f"Found {len(reports)} failure cluster(s):")
    rep = reports[0]
    print(f"  Category       : {rep['category']}")
    print(f"  Confidence     : {round(rep['confidence'] * 100)}%")
    print(f"  Affected Count : {rep['affected_count']}")
    print(f"  Suggested Patch: {json.dumps(rep['suggested_patch'])}")

    print("\nExecuting Redrive with auto-patch back to 'orders-primary'...")
    redrive_res = client.redrive("orders-dlq", "orders-primary", rep["suggested_patch"])
    print(f"✓ Successfully repaired & redriven: {redrive_res['successfully_redriven']} messages")

    # Worker re-consumes repaired messages
    repaired_msgs = client.receive_messages("orders-primary", max_messages=3)
    print(f"Worker reads repaired messages:")
    for rm in repaired_msgs:
        payload = json.loads(rm["body"])
        print(f"  - Cleaned Payload: {payload} (customer_id present: {'customer_id' in payload})")
        client.delete_message("orders-primary", rm["receipt_handle"])
    print("✓ All repaired messages successfully processed and acknowledged!")

    # --- Showcase 4: Deterministic Time-Travel Replay ---
    print_banner("5. DETERMINISTIC TIME-TRAVEL REPLAY")
    now = time.time()
    print("Replaying historical traffic from the past 5 minutes into a shadow queue 'orders-shadow'...")
    replay_res = client.replay(
        source_queue="orders-primary",
        start_time=now - 300,
        end_time=now,
        target_queue="orders-shadow",
        dry_run=False
    )
    print(f"✓ Scanned historical events: {replay_res['total_scanned']}")
    print(f"✓ Replayed into shadow queue: {replay_res['replayed_count']}")

    shadow_check = client.receive_messages("orders-shadow", max_messages=5)
    print(f"✓ Shadow queue contains {len(shadow_check)} replay messages ready for isolated debugging!")

    print_banner("DEMO COMPLETE")
    print(f"The server is running at: http://localhost:{port}/")
    print("Open http://localhost:9324/ in your browser to explore the Web Dashboard!")
    print("Press Ctrl+C to exit when finished.")

    if "--non-interactive" in sys.argv:
        print("Non-interactive run completed successfully.")
        return

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down server...")
        if server:
            server.shutdown()


if __name__ == "__main__":
    main()

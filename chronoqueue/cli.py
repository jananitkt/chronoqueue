"""
ChronoQueue CLI Tool
Command line interface to run server, send/receive messages, and execute replays and triage.
"""

import argparse
import json
import sys
import time
from .client import ChronoClient
from .server import run_server


def main():
    parser = argparse.ArgumentParser(
        prog="chronoqueue",
        description="ChronoQueue: AWS SQS Simulator with Deterministic Time-Travel and Self-Healing DLQ"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Server command
    srv = subparsers.add_parser("server", help="Start the ChronoQueue server")
    srv.add_argument("--port", type=int, default=9324, help="Server port (default: 9324)")
    srv.add_argument("--db", type=str, default="chronoqueue.db", help="SQLite database path")

    # Client subcommands
    create = subparsers.add_parser("create-queue", help="Create a queue")
    create.add_argument("name", help="Queue name")
    create.add_argument("--dlq", help="Dead letter queue name")
    create.add_argument("--fifo", action="store_true", help="Is FIFO queue")
    create.add_argument("--endpoint", default="http://localhost:9324")

    send = subparsers.add_parser("send", help="Send a message")
    send.add_argument("queue", help="Queue name")
    send.add_argument("body", help="Message body (string or JSON)")
    send.add_argument("--group-id", help="FIFO Message Group ID")
    send.add_argument("--dedup-id", help="FIFO Deduplication ID")
    send.add_argument("--endpoint", default="http://localhost:9324")

    recv = subparsers.add_parser("receive", help="Receive messages")
    recv.add_argument("queue", help="Queue name")
    recv.add_argument("--max", type=int, default=1, help="Max messages (default: 1)")
    recv.add_argument("--endpoint", default="http://localhost:9324")

    replay = subparsers.add_parser("replay", help="Execute deterministic time-travel replay")
    replay.add_argument("source_queue", help="Source queue")
    replay.add_argument("--target", help="Target queue (defaults to source)")
    replay.add_argument("--window-seconds", type=int, default=300, help="Seconds in the past (default 300)")
    replay.add_argument("--filter", help="Filter predicate")
    replay.add_argument("--dry-run", action="store_true", help="Perform dry-run only")
    replay.add_argument("--endpoint", default="http://localhost:9324")

    diag = subparsers.add_parser("dlq-diagnose", help="Diagnose quarantined messages in DLQ")
    diag.add_argument("dlq", help="DLQ name")
    diag.add_argument("--endpoint", default="http://localhost:9324")

    args = parser.parse_args()

    if args.command == "server":
        srv = run_server(port=args.port, db_path=args.db)
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down ChronoQueue server...")
            srv.shutdown()
        return

    client = ChronoClient(args.endpoint)

    if args.command == "create-queue":
        res = client.create_queue(args.name, dead_letter_target=args.dlq, is_fifo=args.fifo)
        print("Queue created:", json.dumps(res, indent=2))

    elif args.command == "send":
        res = client.send_message(args.queue, args.body, message_group_id=args.group_id, message_deduplication_id=args.dedup_id)
        print("Message sent:", json.dumps(res, indent=2))

    elif args.command == "receive":
        msgs = client.receive_messages(args.queue, max_messages=args.max)
        print(f"Received {len(msgs)} message(s):")
        for m in msgs:
            print(f"[{m['message_id']}] Body: {m['body']}")

    elif args.command == "replay":
        now = time.time()
        start = now - args.window_seconds
        res = client.replay(
            source_queue=args.source_queue,
            start_time=start,
            end_time=now,
            filter_expr=args.filter,
            target_queue=args.target,
            dry_run=args.dry_run
        )
        print("Replay result:", json.dumps(res, indent=2))

    elif args.command == "dlq-diagnose":
        reports = client.diagnose_dlq(args.dlq)
        print("Diagnosis report:", json.dumps(reports, indent=2))


if __name__ == "__main__":
    main()

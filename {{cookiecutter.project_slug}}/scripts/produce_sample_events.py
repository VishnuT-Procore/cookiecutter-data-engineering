#!/usr/bin/env python3
"""Produce a deterministic sample of customer/order changes to Kafka.

Phase 1 creates 3 customers and 3 orders. Phase 2 updates customer 1, deletes
customer 3, ships order 101, creates order 103 and (Debezium only) replays a stale
update of customer 2 that silver must ignore. Expected state after both phases:

    customers: 1 (alice@new.example.com, Shelbyville), 2 (Bob), 3 deleted
    orders:    100, 101 (shipped), 102, 103

    python scripts/produce_sample_events.py --phase 1
    python scripts/produce_sample_events.py --phase 2
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple

import yaml

ROOT = Path(__file__).resolve().parents[1]
T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)


class Event(NamedTuple):
    entity: str
    key: dict
    value: dict | None


def _at(seconds: int) -> datetime:
    return T0 + timedelta(seconds=seconds)


def _customer(cid, name, email, street, city, country, t):
    return {
        "id": cid,
        "name": name,
        "email": email,
        "street": street,
        "city": city,
        "country": country,
        "updated_at": _at(t).isoformat().replace("+00:00", "Z"),
    }


def _order(oid, cid, status, amount, t):
    return {
        "order_id": oid,
        "customer_id": cid,
        "status": status,
        "amount": amount,
        "updated_at": _at(t).isoformat().replace("+00:00", "Z"),
    }


ALICE = _customer(1, "Alice", "alice@example.com", "1 Main St", "Springfield", "US", 0)
BOB = _customer(2, "Bob", "bob@example.com", "2 Oak Ave", "Portland", "US", 1)
CAROL = _customer(3, "Carol", "carol@example.com", "3 Elm Rd", "Toronto", "CA", 2)
O100 = _order(100, 1, "created", 25.50, 10)
O101 = _order(101, 2, "created", 10.00, 11)
O102 = _order(102, 1, "created", 99.99, 12)

# (entity, op, before, after, seconds since T0, debezium_only)
CHANGES = {
    1: [
        ("customers", "c", None, ALICE, 0, False),
        ("customers", "c", None, BOB, 1, False),
        ("customers", "c", None, CAROL, 2, False),
        ("orders", "c", None, O100, 10, False),
        ("orders", "c", None, O101, 11, False),
        ("orders", "c", None, O102, 12, False),
    ],
    2: [
        ("customers", "u", ALICE,
         {**ALICE, "email": "alice@new.example.com", "city": "Shelbyville", "updated_at": "2024-01-01T00:01:00Z"},
         60, False),
        # Out-of-order replay with an older source timestamp: silver must keep "Bob".
        ("customers", "u", BOB, {**BOB, "name": "Bob (stale)", "updated_at": "2023-12-31T23:59:00Z"}, -60, True),
        ("customers", "d", CAROL, None, 65, False),
        ("orders", "u", O101, {**O101, "status": "shipped", "updated_at": "2024-01-01T00:01:10Z"}, 70, False),
        ("orders", "c", None, _order(103, 2, "created", 5.00, 75), 75, False),
    ],
}

PRIMARY_KEYS = {"customers": ["id"], "orders": ["order_id"]}


def sample_events(payload_format: str, phase: int) -> list[Event]:
    """Return the Kafka (key, value) events for a phase in the given payload format."""
    events: list[Event] = []
    for entity, op, before, after, t, debezium_only in CHANGES[phase]:
        if debezium_only and payload_format != "debezium":
            continue
        row = after or before
        key = {k: row[k] for k in PRIMARY_KEYS[entity]}
        if payload_format == "debezium":
            envelope = {
                "before": before,
                "after": after,
                "op": op,
                "ts_ms": int(_at(t).timestamp() * 1000),
                "source": {"connector": "postgresql", "db": "app", "schema": "public", "table": entity},
            }
            events.append(Event(entity, key, envelope))
            if op == "d":
                events.append(Event(entity, key, None))  # Debezium tombstone for log compaction
        else:
            events.append(Event(entity, key, None if op == "d" else after))
    return events


def encode(obj: dict | None) -> bytes | None:
    return None if obj is None else json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()


def kafka_config() -> dict:
    """librdkafka config from KAFKA_BOOTSTRAP_SERVERS plus any KAFKA_CONF_<KEY> variables."""
    conf = {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")}
    for name, value in os.environ.items():
        if name.startswith("KAFKA_CONF_"):
            conf[name[len("KAFKA_CONF_"):].lower().replace("_", ".")] = value
    return conf


def produce(phases: list[int], bootstrap_servers: str | None = None) -> int:
    from confluent_kafka import Producer

    with open(ROOT / "config" / "pipeline.yml") as f:
        pipeline = yaml.safe_load(f)
    topics = {e["name"]: e["topic"] for e in pipeline["entities"]}

    conf = kafka_config()
    if bootstrap_servers:
        conf["bootstrap.servers"] = bootstrap_servers
    producer = Producer(conf)
    count = 0
    for phase in phases:
        for event in sample_events(pipeline["payload_format"], phase):
            if event.entity not in topics:
                continue
            producer.produce(topics[event.entity], key=encode(event.key), value=encode(event.value))
            count += 1
    producer.flush(30)
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--phase", choices=["1", "2", "all"], default="all")
    args = parser.parse_args()
    phases = [1, 2] if args.phase == "all" else [int(args.phase)]
    print(f"produced {produce(phases)} events")
    return 0


if __name__ == "__main__":
    sys.exit(main())

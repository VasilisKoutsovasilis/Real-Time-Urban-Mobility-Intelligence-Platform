"""Generate deterministic synthetic traffic-sensor events as JSON Lines."""

from __future__ import annotations

import argparse
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, TextIO


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and require an explicit timezone."""
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        timestamp = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "start time must be ISO-8601, e.g. 2026-01-01T00:00:00Z"
        ) from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise argparse.ArgumentTypeError(
            "start time must include a timezone, e.g. 2026-01-01T00:00:00Z"
        )
    return timestamp.astimezone(timezone.utc)


def timestamp_text(value: datetime) -> str:
    """Serialize a timezone-aware datetime as UTC ISO-8601 with a Z suffix."""
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def generate_events(
    count: int,
    sensor_count: int = 20,
    seed: int = 42,
    start_time: datetime | None = None,
) -> Iterator[dict[str, object]]:
    """Yield reproducible traffic events with one-second event-time spacing.

    Reusing the same arguments produces the same events. This makes fixtures and
    later replay/benchmark experiments reproducible. Event IDs are stable for a
    given generated sequence and are not intended to identify real-world events.
    """
    if count < 0:
        raise ValueError("count must be >= 0")
    if sensor_count < 1:
        raise ValueError("sensor_count must be >= 1")

    base_time = start_time or datetime.now(timezone.utc)
    if base_time.tzinfo is None or base_time.utcoffset() is None:
        raise ValueError("start_time must be timezone-aware")
    base_time = base_time.astimezone(timezone.utc)
    rng = random.Random(seed)

    for index in range(count):
        sensor_number = rng.randrange(sensor_count) + 1
        vehicle_count = rng.randint(0, 45)
        # Simulate lower speeds during busier periods, with some random variation.
        avg_speed = max(5.0, min(110.0, 92.0 - 1.65 * vehicle_count + rng.gauss(0, 7)))
        occupancy = max(0.0, min(100.0, vehicle_count * 2.1 + rng.uniform(-5, 8)))
        event_time = base_time + timedelta(seconds=index)

        yield {
            "schema_version": 1,
            "event_id": f"evt-{index:012d}",
            "sensor_id": f"sensor-{sensor_number:04d}",
            "segment_id": f"road-{sensor_number:04d}",
            "event_time": timestamp_text(event_time),
            "vehicle_count": vehicle_count,
            "avg_speed_kmh": round(avg_speed, 2),
            "occupancy_pct": round(occupancy, 2),
        }


def write_jsonl(events: Iterator[dict[str, object]], output: TextIO) -> int:
    """Write events as compact JSON Lines; return the number written."""
    written = 0
    for event in events:
        output.write(json.dumps(event, separators=(",", ":"), sort_keys=True))
        output.write("\n")
        written += 1
    return written


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate reproducible synthetic traffic-sensor events (JSONL)."
    )
    parser.add_argument("--count", type=int, default=1000, help="number of events (default: 1000)")
    parser.add_argument("--sensor-count", type=int, default=20, help="number of sensors (default: 20)")
    parser.add_argument("--seed", type=int, default=42, help="random seed (default: 42)")
    parser.add_argument(
        "--start-time",
        type=parse_timestamp,
        default=None,
        help="UTC/ISO-8601 starting event time, e.g. 2026-01-01T00:00:00Z",
    )
    parser.add_argument(
        "--output",
        default="-",
        help="output JSONL path; use '-' for stdout (default: '-')",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.count < 0:
        parser.error("--count must be >= 0")
    if args.sensor_count < 1:
        parser.error("--sensor-count must be >= 1")

    events = generate_events(
        count=args.count,
        sensor_count=args.sensor_count,
        seed=args.seed,
        start_time=args.start_time,
    )

    if args.output == "-":
        write_jsonl(events, sys.stdout)
    else:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8", newline="\n") as output_file:
            write_jsonl(events, output_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

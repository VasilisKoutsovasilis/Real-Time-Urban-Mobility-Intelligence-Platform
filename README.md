# Real-Time Mobility Streaming Platform

A local-first streaming data engineering project using Python, Apache Kafka, Apache Flink, and PostgreSQL. The goal is to demonstrate event-time processing, stateful stream processing, replay, fault tolerance, observability, and reproducible performance experiments.

## Current milestone: Step 1 — Event contract and synthetic generator

This first milestone intentionally uses only the Python standard library. Kafka, Flink, PostgreSQL, and Docker Compose will be added in later steps after the event contract is tested.

### Requirements

- Python 3.10 or newer
- Git (recommended)

### Generate a small sample

From the repository root:

```bash
python -m mobility_streaming.generator \
  --count 100 \
  --sensor-count 10 \
  --seed 42 \
  --start-time 2026-01-01T00:00:00Z \
  --output data/events.jsonl
```

This creates `data/events.jsonl`, one JSON object per line. The seed and start time make the output reproducible. The generated data file is ignored by Git.

To print JSONL to stdout instead:

```bash
python -m mobility_streaming.generator --count 5 --start-time 2026-01-01T00:00:00Z
```

### Run tests

```bash
python -m unittest discover -s tests -v
```

### Event contract, version 1

| Field | Type | Meaning |
|---|---|---|
| `schema_version` | integer | Event schema version; currently `1` |
| `event_id` | string | Stable ID for a generated sequence, useful for deduplication experiments |
| `sensor_id` | string | Synthetic sensor identifier |
| `segment_id` | string | Road segment associated with the sensor |
| `event_time` | ISO-8601 UTC string | Time the event represents, not the time it is processed |
| `vehicle_count` | integer | Vehicles counted during the sample interval |
| `avg_speed_kmh` | number | Simulated average speed in km/h |
| `occupancy_pct` | number | Simulated road occupancy percentage |

The schema lives at `schemas/traffic_event.schema.json`. The generator emits increasing event timestamps in one-second increments. Later milestones will deliberately introduce duplicates, out-of-order events, malformed records, and delays to test pipeline behavior.

## Planned milestones

1. [x] Define event contract and deterministic generator
2. [ ] Containerized Kafka and topic bootstrap
3. [ ] Kafka producer and input validation
4. [ ] Java Flink event-time window aggregation
5. [ ] PostgreSQL sink with idempotent writes
6. [ ] Checkpoint/recovery and replay tests
7. [ ] Metrics, dashboards, and benchmark harness
8. [ ] CI, architecture diagram, and reproducible documentation

## Project principles

- Local development must not require paid cloud services or external API keys.
- Prefer reproducible experiments over unsupported performance claims.
- Document the actual delivery guarantees; do not equate Flink checkpoints with end-to-end exactly-once sink semantics.

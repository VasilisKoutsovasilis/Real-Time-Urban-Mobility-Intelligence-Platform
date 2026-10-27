import json
import unittest
from datetime import datetime, timezone

from mobility_streaming.generator import generate_events, timestamp_text, write_jsonl


class GenerateEventsTests(unittest.TestCase):
    def setUp(self):
        self.start_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def test_generates_requested_count(self):
        events = list(generate_events(17, start_time=self.start_time))
        self.assertEqual(len(events), 17)

    def test_same_seed_is_reproducible(self):
        first = list(generate_events(50, sensor_count=7, seed=123, start_time=self.start_time))
        second = list(generate_events(50, sensor_count=7, seed=123, start_time=self.start_time))
        self.assertEqual(first, second)

    def test_event_ids_are_unique_and_sequential(self):
        events = list(generate_events(5, start_time=self.start_time))
        self.assertEqual([e["event_id"] for e in events], [f"evt-{i:012d}" for i in range(5)])

    def test_required_fields_and_ranges(self):
        expected = {
            "schema_version", "event_id", "sensor_id", "segment_id", "event_time",
            "vehicle_count", "avg_speed_kmh", "occupancy_pct"
        }
        events = list(generate_events(100, sensor_count=4, start_time=self.start_time))
        for event in events:
            self.assertEqual(set(event), expected)
            self.assertEqual(event["schema_version"], 1)
            self.assertGreaterEqual(event["vehicle_count"], 0)
            self.assertLessEqual(event["vehicle_count"], 45)
            self.assertGreaterEqual(event["avg_speed_kmh"], 5)
            self.assertLessEqual(event["avg_speed_kmh"], 110)
            self.assertGreaterEqual(event["occupancy_pct"], 0)
            self.assertLessEqual(event["occupancy_pct"], 100)

    def test_event_time_increases_by_one_second(self):
        events = list(generate_events(3, start_time=self.start_time))
        self.assertEqual(
            [e["event_time"] for e in events],
            [
                "2026-01-01T00:00:00.000Z",
                "2026-01-01T00:00:01.000Z",
                "2026-01-01T00:00:02.000Z",
            ],
        )

    def test_write_jsonl_writes_one_json_object_per_line(self):
        from io import StringIO

        buffer = StringIO()
        count = write_jsonl(generate_events(3, start_time=self.start_time), buffer)
        lines = buffer.getvalue().splitlines()
        self.assertEqual(count, 3)
        self.assertEqual(len(lines), 3)
        self.assertTrue(all(isinstance(json.loads(line), dict) for line in lines))

    def test_rejects_naive_start_time(self):
        with self.assertRaises(ValueError):
            list(generate_events(1, start_time=datetime(2026, 1, 1)))

    def test_rejects_invalid_counts(self):
        with self.assertRaises(ValueError):
            list(generate_events(-1, start_time=self.start_time))
        with self.assertRaises(ValueError):
            list(generate_events(1, sensor_count=0, start_time=self.start_time))


if __name__ == "__main__":
    unittest.main()

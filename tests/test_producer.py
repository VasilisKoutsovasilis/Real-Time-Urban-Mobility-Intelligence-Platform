import unittest
import json
from kafka_producer import delivery_report, produce_events, delivery_report


class FakeProducer:
    def __init__(self):
        self.messages = []
        self.poll_calls = 0
        
    def produce(self, topic, key, value, on_delivery):
        self.messages.append({
            "topic": topic,
            "key": key,
            "value": value,
            "on_delivery": on_delivery
        })
        
    def poll(self, timeout):
        self.poll_calls += 1
        
    def flush(self, timeout):
        return 0
        
class PublishEventsTest(unittest.TestCase):
    def test_publishes_one_message_per_event(self):
        producer = FakeProducer()
        stats = {"success": 0, "errors": 0}
        
        events = [
            {"event_id": "evt-1", "sensor_id": "sensor-1"},
            {"event_id": "evt-2", "sensor_id": "sensor-2"},
        ]
        
        count = produce_events(events, producer, stats, "traffic-events")
        self.assertEqual(count, 2)
        self.assertEqual(len(producer.messages), 2)
        self.assertEqual(
            [message['topic'] for message in producer.messages],
            ['traffic-events', 'traffic-events']
        )
        self.assertEqual(
            [event['sensor_id'] for event in events],
            [message['key'].decode('utf-8') for message in producer.messages]
        )
        self.assertEqual(
            json.loads(producer.messages[0]['value']),
            events[0]
        )
        
    def test_delivery_report_counts_success(self):
        stats = {"success": 0, "errors": 0}

        delivery_report(None, None, stats)

        self.assertEqual(stats["success"], 1)
        self.assertEqual(stats["errors"], 0)

    def test_delivery_report_counts_errors(self):
        stats = {"success": 0, "errors": 0}
        
        delivery_report("Error", None, stats)
        
        self.assertEqual(stats["success"], 0)
        self.assertEqual(stats["errors"], 1)
        
if __name__ == "__main__":
    unittest.main()
import json
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

metadata = producer.list_topics(timeout=10)

print("Successfully connected to kafka...")
print("Available topics: ")

for topic in sorted(metadata.topics):
    print(f" - {topic}")
    
event = {
    "schema_version": "1",
    "event_id": "evt-python-0001",
    "sensor_id": "sensor-0001",
    "segment_id": "road-0001",
    "event_time": "2026-01-01T00:00:00.000Z",
    "vehicle_count": "12",
    "avg_speed_kmh": "34.5",
    "occupancy_pct": "22.1"
}

producer.produce(
    topic="traffic-events",
    key=event['sensor_id'].encode('utf-8'),
    value=json.dumps(event).encode('utf-8')
)

producer.flush()

print("Event published successfully!")
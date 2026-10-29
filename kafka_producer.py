import json
from confluent_kafka import Producer
import mobility_streaming.generator as gen
import sys
from pathlib import Path

def delivery_report(err, msg, stats):
    
    if err is not None:
        stats["errors"] += 1
        print(f"Delivery failed with error: {err}")
    else:
        stats["success"] += 1

def produce_events(events, producer, stats, topic):
    records_sent = 0
    for event in events:
        producer.produce(
            topic=topic,
            key=str(event['sensor_id']).encode('utf-8'),
            value=json.dumps(event).encode('utf-8'),
            on_delivery=lambda err, msg: delivery_report(err, msg, stats)
        )
    
        records_sent += 1
    
        if records_sent % 100 == 0:
            producer.poll(0)
    
    remaining = producer.flush(timeout=30)
    
    print(f"Records attempted: {records_sent}")
    print(f"Records successfully delivered: {stats["success"]}")
    print(f"Records failed to be delivered: {stats["errors"]}")
    print(f"Records pending: {remaining}")
    
    if remaining > 0 or stats["errors"] > 0:
        print("Delivery finished with problems...")
        return -1
    
    print("All records successfully delivered to kafka!")
    return records_sent
    
def main(argv: list[str] | None = None):
    parser = gen.build_parser()
    args = parser.parse_args(argv)
    if args.count < 0:
        parser.error("--count must be >= 0")
    if args.sensor_count < 1:
        parser.error("--sensor-count must be >= 1")
    
    producer = Producer({
        "bootstrap.servers": "localhost:9092"
    })

    events = gen.generate_events(
        count=args.count,
        sensor_count=args.sensor_count,
        seed=args.seed,
        start_time=args.start_time
    )
    
    if args.output != "-":
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8", newline="\n") as output_file:
            gen.write_jsonl(events, output_file)
    
    
    stats = {"success": 0, "errors": 0}
    return produce_events(events, producer, stats, "traffic-events")
    
if __name__ == "__main__":
    raise SystemExit(main())
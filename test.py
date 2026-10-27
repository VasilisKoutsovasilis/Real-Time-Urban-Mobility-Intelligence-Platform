import json

with open(r"D:\GraduateProjects\realtime-mobility-streaming-platform-step1\data\events.jsonl", 'r') as f:
    for line in f:
        sensor_reading = json.loads(line.strip())
        print(sensor_reading)
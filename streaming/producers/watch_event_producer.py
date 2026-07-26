"""
Netflix-style watch event Kafka producer.
Generates realistic watch-events and late ratings continuously.
"""
import json
import random
import time
import uuid
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
WATCH_EVENTS_TOPIC = "watch-events"
RATINGS_LATE_TOPIC = "ratings-late"

USERS      = [f"user_{i:03d}" for i in range(1, 101)]
CONTENT    = [f"content_{i:03d}" for i in range(1, 51)]
DEVICES    = ["mobile", "desktop", "tablet", "smart_tv"]
EVENT_TYPES = ["play", "pause", "stop", "finish"]


def make_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
            print("Connected to Kafka")
            return p
        except Exception as e:
            print(f"Waiting for Kafka... {e}")
            time.sleep(5)


def watch_event():
    now = datetime.now(timezone.utc).isoformat()
    return {
        "event_id":               str(uuid.uuid4()),
        "user_id":                random.choice(USERS),
        "content_id":             random.choice(CONTENT),
        "event_type":             random.choice(EVENT_TYPES),
        "device_type":            random.choice(DEVICES),
        "session_id":             str(uuid.uuid4()),
        "watch_duration_seconds": random.randint(30, 7200),
        "event_time":             now,
        "ingestion_time":         now,
    }


def late_rating():
    event_time = datetime.now(timezone.utc) - timedelta(hours=random.uniform(1, 48))
    return {
        "rating_id":      str(uuid.uuid4()),
        "user_id":        random.choice(USERS),
        "content_id":     random.choice(CONTENT),
        "rating_value":   random.randint(1, 5),
        "event_time":     event_time.isoformat(),
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = make_producer()
    count = 0
    print("Producing events... (Ctrl+C to stop)")
    while True:
        ev = watch_event()
        producer.send(WATCH_EVENTS_TOPIC, key=ev["user_id"], value=ev)
        count += 1

        if count % 5 == 0:
            r = late_rating()
            producer.send(RATINGS_LATE_TOPIC, key=r["user_id"], value=r)

        if count % 20 == 0:
            producer.flush()
            print(f"Sent {count} watch-events + {count // 5} late-ratings")

        time.sleep(random.uniform(0.3, 1.5))


if __name__ == "__main__":
    main()

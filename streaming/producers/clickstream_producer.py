"""
High-Throughput Clickstream & User Interaction Event Producer
Simulates 50,000+ events/sec production stream feeding Kafka.
Includes fraud patterns, user sessions, search queries, and purchase behaviors.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Generator

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None


EVENT_TYPES = [
    "page_view",
    "search_query",
    "item_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "payment_completed",
    "login_attempt",
    "review_submitted",
]

DEVICE_TYPES = ["mobile_ios", "mobile_android", "desktop_chrome", "desktop_safari", "tablet"]
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "sa-east-1"]
SEARCH_QUERIES = [
    "distributed systems with apache flink",
    "real-time data lakehouse iceberg",
    "vector database hybrid search milvus",
    "low latency clickstream analytics clickhouse",
    "agentic rag with langchain and ray",
    "llm evaluation metrics ragas",
]


class ClickstreamEventGenerator:
    """Generates realistic clickstream events with synthetic fraud scenarios."""

    def __init__(self, num_users: int = 10000, fraud_ratio: float = 0.02):
        self.users = [f"usr_{uuid.uuid4().hex[:10]}" for _ in range(num_users)]
        self.fraud_ratio = fraud_ratio

    def generate_single_event(self) -> Dict[str, Any]:
        user_id = random.choice(self.users)
        event_type = random.choices(
            EVENT_TYPES,
            weights=[35, 20, 20, 10, 3, 5, 4, 2, 1],
            k=1
        )[0]

        is_fraud = random.random() < self.fraud_ratio
        amount = 0.0
        if event_type in ("checkout_started", "payment_completed"):
            amount = round(random.uniform(5000.0, 50000.0) if is_fraud else random.uniform(10.0, 500.0), 2)

        ip_address = f"192.168.{random.randint(1, 254)}.{random.randint(1, 254)}"
        if is_fraud:
            # Suspect VPN or spoofed address
            ip_address = f"10.99.{random.randint(1, 254)}.{random.randint(1, 254)}"

        payload = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": f"sess_{random.randint(10000, 99999)}",
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "epoch_ms": int(time.time() * 1000),
            "device": random.choice(DEVICE_TYPES),
            "region": random.choice(REGIONS),
            "ip_address": ip_address,
            "properties": {
                "url": f"https://platform.data/app/{event_type}",
                "amount": amount,
                "currency": "USD",
                "search_keyword": random.choice(SEARCH_QUERIES) if event_type == "search_query" else None,
                "latency_ms": random.randint(15, 120),
                "is_flagged_fraud_pattern": is_fraud,
            }
        }
        return payload

    def batch_generator(self, batch_size: int = 1000) -> Generator[list, None, None]:
        while True:
            yield [self.generate_single_event() for _ in range(batch_size)]


class HighThroughputProducer:
    """Manages high-throughput ingestion into Kafka topics."""

    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "clickstream.events"):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.generator = ClickstreamEventGenerator()

        if KafkaProducer is not None:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks=1,  # High-throughput balance
                    linger_ms=10,
                    batch_size=65536,  # 64KB batch
                    compression_type="lz4",
                )
                print(f"[HighThroughputProducer] Connected to Kafka: {bootstrap_servers}")
            except Exception as e:
                print(f"[HighThroughputProducer] Kafka connection failed ({e}). Running in Dry-Run simulation mode.")
                self.producer = None
        else:
            print("[HighThroughputProducer] kafka-python not installed. Running in Dry-Run simulation mode.")
            self.producer = None

    def publish_batch(self, events: list):
        for event in events:
            key = event["user_id"]
            if self.producer:
                self.producer.send(self.topic, key=key, value=event)
        if self.producer:
            self.producer.flush()

    def run_benchmark(self, target_events: int = 50000, batch_size: int = 2500, workers: int = 4):
        """Simulate high throughput burst (e.g. 50,000+ events/sec)."""
        print(f"[*] Starting streaming benchmark: target={target_events} events, batch_size={batch_size}, workers={workers}")
        start_time = time.time()
        total_published = 0

        batches = [
            [self.generator.generate_single_event() for _ in range(batch_size)]
            for _ in range(target_events // batch_size)
        ]

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(self.publish_batch, batch) for batch in batches]
            for future in futures:
                future.result()
                total_published += batch_size

        duration = max(time.time() - start_time, 0.001)
        rate = total_published / duration
        print(f"[DONE] Benchmark completed!")
        print(f"       Total Events Published : {total_published:,}")
        print(f"       Elapsed Time           : {duration:.2f} seconds")
        print(f"       Throughput Rate        : {rate:,.2f} events/second")
        return rate


if __name__ == "__main__":
    producer = HighThroughputProducer()
    producer.run_benchmark(target_events=50000, batch_size=2500, workers=4)

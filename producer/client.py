"""Aquanode marketplace API -> Kafka."""

import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

from producer.config import API_KEY, API_URL


class StreamingDataProducer:
    def __init__(self, bootstrap_servers: str, topic: str, gpu_limit: int):
        self.topic = topic
        self.gpu_limit = gpu_limit
        self.session = requests.Session()
        if API_KEY:
            self.session.headers["Authorization"] = f"Bearer {API_KEY}"
        self.producer = self._connect(bootstrap_servers)

    @staticmethod
    def _connect(bootstrap_servers: str, attempts: int = 30) -> KafkaProducer:
        """Kafka can take a while to start inside Docker, so retry instead of crashing."""
        for attempt in range(1, attempts + 1):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    retries=5,
                )
                print(f"Kafka producer connected to {bootstrap_servers}", flush=True)
                return producer
            except KafkaError:  # NoBrokersAvailable in kafka-python 2.x, other connection errors in 3.x
                print(f"Kafka not ready ({attempt}/{attempts}), retrying in 5s...", flush=True)
                time.sleep(5)
        print("ERROR: could not reach Kafka", flush=True)
        sys.exit(1)

    def fetch_offers(self) -> List[Dict[str, Any]]:
        """Call the API once and return one record per GPU model (cheapest offer)."""
        try:
            response = self.session.get(API_URL, timeout=15)
            if response.status_code in (401, 403) and "Authorization" in self.session.headers:
                # the marketplace endpoint is public, so a rejected key must not stop the stream
                print("API key was rejected - continuing without it", flush=True)
                del self.session.headers["Authorization"]
                response = self.session.get(API_URL, timeout=15)
            response.raise_for_status()
            offers = response.json().get("data", [])
        except Exception as exc:
            print(f"API error: {exc}", flush=True)
            return []

        best: Dict[str, Dict[str, Any]] = {}
        seen = Counter()
        for offer in offers:
            name = offer.get("gpuShortName")
            price = offer.get("price")
            if not name or price is None:
                continue
            seen[name] += 1
            if name not in best or float(price) < best[name]["value"]:
                best[name] = {
                    "value": float(price),
                    "metric_type": "GPU_Price",
                    "sensor_id": name,
                    "provider": offer.get("provider", "unknown"),
                    "unit": "USD_per_hour",
                }

        # Track the most widely offered GPU models so the same GPUs appear every poll
        tracked = [name for name, _ in seen.most_common(self.gpu_limit)]
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        return [{"timestamp": now, **best[name]} for name in sorted(tracked)]

    def run(self, interval: float, duration: int = None):
        print(
            f"Producing to '{self.topic}' every {interval:.0f}s " f"(up to {self.gpu_limit} GPU models per poll)",
            flush=True,
        )
        start, polls, sent = time.time(), 0, 0
        try:
            while not duration or time.time() - start < duration:
                records = self.fetch_offers()
                for record in records:
                    try:
                        self.producer.send(self.topic, value=record).get(timeout=10)
                        sent += 1
                    except KafkaError as exc:
                        print(f"Kafka send error: {exc}", flush=True)
                polls += 1
                print(f"[poll {polls}] sent {len(records)} messages (total {sent})", flush=True)
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\nProducer interrupted by user", flush=True)
        finally:
            self.producer.flush(timeout=10)
            self.producer.close()
            print(f"Producer stopped. Total messages sent: {sent}", flush=True)

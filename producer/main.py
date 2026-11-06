"""Command-line entry point:  python -m producer"""

import argparse
import os

from producer.client import StreamingDataProducer


def parse_arguments():
    parser = argparse.ArgumentParser(description="Kafka streaming producer (GPU prices)")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "gpu_stream"))
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.getenv("PRODUCER_INTERVAL", "15")),
        help="seconds between API polls (default 15)",
    )
    parser.add_argument(
        "--gpu-limit",
        type=int,
        default=int(os.getenv("PRODUCER_GPU_LIMIT", "8")),
        help="how many GPU models to publish per poll (default 8)",
    )
    parser.add_argument("--duration", type=int, default=None, help="run time in seconds (default: forever)")
    return parser.parse_args()


def main():
    print("=" * 60)
    print("STREAMING DATA PRODUCER - CLOUD GPU PRICES")
    print("=" * 60, flush=True)
    args = parse_arguments()
    StreamingDataProducer(args.bootstrap_servers, args.topic, args.gpu_limit).run(args.interval, args.duration)

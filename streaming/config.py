"""Job configuration (environment variables)."""

import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "gpu_stream")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "bigdata_db")
HISTORY_COLLECTION = os.getenv("MONGO_HISTORY_COLLECTION", "gpu_history")
AGG_COLLECTION = os.getenv("MONGO_AGG_COLLECTION", "gpu_aggregates")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints")
WINDOW = os.getenv("AGG_WINDOW", "1 minute")
TRIGGER = os.getenv("SPARK_TRIGGER", "10 seconds")
SPARK_KAFKA_PACKAGE = os.getenv("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")

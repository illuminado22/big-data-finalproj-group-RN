"""
Spark Structured Streaming job:  Kafka  ->  Spark  ->  MongoDB

  gpu_stream (Kafka)
      |-- stream 1: every message  -> MongoDB  gpu_history     (full history, de-duplicated)
      '-- stream 2: 1-minute windows per GPU (avg/min/max/count/stddev)
                                   -> MongoDB  gpu_aggregates  (data aggregation)

The job is idempotent: records are upserted on a natural key, so a restart or a replayed
batch never creates duplicates.
"""

import os

from pyspark.sql import SparkSession

from streaming.config import (
    AGG_COLLECTION,
    CHECKPOINT_DIR,
    HISTORY_COLLECTION,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    MONGO_DB,
    SPARK_KAFKA_PACKAGE,
    TRIGGER,
)
from streaming.mongo_sink import ensure_indexes, write_aggregates, write_history
from streaming.transform import aggregate, parse_stream


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("gpu-price-streaming")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def main():
    ensure_indexes()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = parse_stream(raw)

    (
        parsed.writeStream.foreachBatch(write_history)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/history")
        .trigger(processingTime=TRIGGER)
        .start()
    )
    (
        aggregate(parsed)
        .writeStream.outputMode("update")
        .foreachBatch(write_aggregates)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/aggregates")
        .trigger(processingTime=TRIGGER)
        .start()
    )
    print(f"Spark streaming started: {KAFKA_TOPIC} -> {MONGO_DB}.{HISTORY_COLLECTION} / {AGG_COLLECTION}", flush=True)
    spark.streams.awaitAnyTermination()

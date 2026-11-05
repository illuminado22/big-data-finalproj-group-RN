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
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

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

MESSAGE_SCHEMA = StructType([
    StructField("timestamp", StringType()),
    StructField("value", DoubleType()),
    StructField("metric_type", StringType()),
    StructField("sensor_id", StringType()),
    StructField("provider", StringType()),
    StructField("unit", StringType()),
])


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


def mongo_collection(name):
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    return client, client[MONGO_DB][name]


def parse_stream(raw):
    """Kafka value (bytes) -> typed columns."""
    return (
        raw.select(F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("m"))
        .select("m.*")
        .withColumn("timestamp", F.col("timestamp").cast("timestamp"))
        .dropna(subset=["timestamp", "value", "sensor_id"])
    )


def utc(ms):
    """Epoch milliseconds -> naive UTC datetime. (Spark's collect() would use the host timezone.)"""
    return datetime.fromtimestamp(ms / 1000, timezone.utc).replace(tzinfo=None)


# ---------- sinks (foreachBatch) ----------
def write_history(batch_df, batch_id):
    rows = batch_df.withColumn("ts_ms", F.unix_millis("timestamp")).collect()
    if not rows:
        return
    ops = []
    for r in rows:
        key = {"timestamp": utc(r["ts_ms"]), "sensor_id": r["sensor_id"], "provider": r["provider"]}
        doc = {**key, "value": r["value"], "metric_type": r["metric_type"], "unit": r["unit"]}
        ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
    client, col = mongo_collection(HISTORY_COLLECTION)
    try:
        col.bulk_write(ops, ordered=False)
    finally:
        client.close()
    print(f"[history] batch {batch_id}: upserted {len(ops)} records", flush=True)


def write_aggregates(batch_df, batch_id):
    rows = (
        batch_df.withColumn("start_ms", F.unix_millis(F.col("window.start")))
        .withColumn("end_ms", F.unix_millis(F.col("window.end")))
        .collect()
    )
    if not rows:
        return
    ops = []
    for r in rows:
        key = {"window_start": utc(r["start_ms"]), "sensor_id": r["sensor_id"]}
        doc = {
            **key,
            "window_end": utc(r["end_ms"]),
            "avg_price": r["avg_price"],
            "min_price": r["min_price"],
            "max_price": r["max_price"],
            "std_price": r["std_price"] or 0.0,
            "samples": int(r["samples"]),
        }
        ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
    client, col = mongo_collection(AGG_COLLECTION)
    try:
        col.bulk_write(ops, ordered=False)
    finally:
        client.close()
    print(f"[aggregates] batch {batch_id}: upserted {len(ops)} windows", flush=True)


def aggregate(parsed):
    return (
        parsed.withWatermark("timestamp", "2 minutes")
        .dropDuplicates(["sensor_id", "provider", "timestamp"])   # replayed messages must not count twice
        .groupBy(F.window("timestamp", WINDOW), "sensor_id")
        .agg(
            F.avg("value").alias("avg_price"),
            F.min("value").alias("min_price"),
            F.max("value").alias("max_price"),
            F.stddev_pop("value").alias("std_price"),
            F.count("*").alias("samples"),
        )
    )


def ensure_indexes():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30000)
    db = client[MONGO_DB]
    db[HISTORY_COLLECTION].create_index([("timestamp", 1), ("sensor_id", 1), ("provider", 1)], unique=True)
    db[HISTORY_COLLECTION].create_index([("sensor_id", 1), ("timestamp", -1)])
    db[AGG_COLLECTION].create_index([("window_start", 1), ("sensor_id", 1)], unique=True)
    client.close()


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

    history_q = (
        parsed.writeStream.foreachBatch(write_history)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/history")
        .trigger(processingTime=TRIGGER)
        .start()
    )
    agg_q = (
        aggregate(parsed).writeStream.outputMode("update")
        .foreachBatch(write_aggregates)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/aggregates")
        .trigger(processingTime=TRIGGER)
        .start()
    )
    print(f"Spark streaming started: {KAFKA_TOPIC} -> {MONGO_DB}.{HISTORY_COLLECTION} / {AGG_COLLECTION}", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

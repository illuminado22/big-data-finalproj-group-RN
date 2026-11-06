"""MongoDB sinks (foreachBatch): idempotent upserts on natural keys."""

from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne
from pyspark.sql import functions as F

from streaming.config import AGG_COLLECTION, HISTORY_COLLECTION, MONGO_DB, MONGO_URI


def mongo_collection(name):
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    return client, client[MONGO_DB][name]


def utc(ms):
    """Epoch milliseconds -> naive UTC datetime. (Spark's collect() would use the host timezone.)"""
    return datetime.fromtimestamp(ms / 1000, timezone.utc).replace(tzinfo=None)


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


def ensure_indexes():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30000)
    db = client[MONGO_DB]
    db[HISTORY_COLLECTION].create_index([("timestamp", 1), ("sensor_id", 1), ("provider", 1)], unique=True)
    db[HISTORY_COLLECTION].create_index([("sensor_id", 1), ("timestamp", -1)])
    db[AGG_COLLECTION].create_index([("window_start", 1), ("sensor_id", 1)], unique=True)
    client.close()

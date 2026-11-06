"""Data access: Kafka (live view) and MongoDB (history written by Spark)."""

import json

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer, TopicPartition
from pymongo import MongoClient

from dashboard.config import (
    AGG_COLLECTION,
    COLLECTION_NAME,
    DB_NAME,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    LIVE_MESSAGES,
    MONGO_URI,
)


def consume_kafka_data():
    """Latest LIVE_MESSAGES messages of the topic -> (DataFrame, error message or None)."""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=2000,
        )
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        if not partitions:
            consumer.close()
            return pd.DataFrame(), f"Topic '{KAFKA_TOPIC}' does not exist yet - waiting for the producer."
        tps = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
        consumer.assign(tps)
        ends = consumer.end_offsets(tps)
        per_partition = max(1, LIVE_MESSAGES // len(tps))
        for tp in tps:
            consumer.seek(tp, max(0, ends[tp] - per_partition))

        messages = [msg.value for msg in consumer]
        consumer.close()

        if messages:
            df = pd.DataFrame(messages)
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            return df, None
        return pd.DataFrame(), None
    except Exception as exc:
        return pd.DataFrame(), f"{type(exc).__name__}: {exc}"


def _read_collection(name, time_cols, sort_col):
    """Whole MongoDB collection -> (DataFrame, error message or None)."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        data = list(client[DB_NAME][name].find({}, {"_id": 0}))
        client.close()
    except Exception as exc:
        return pd.DataFrame(), f"{type(exc).__name__}: {exc}"
    if not data:
        return pd.DataFrame(), None
    df = pd.DataFrame(data)
    for col in time_cols:
        df[col] = pd.to_datetime(df[col], utc=True)
    return df.sort_values(sort_col).reset_index(drop=True), None


@st.cache_data(ttl=30, show_spinner=False)
def query_mongodb():
    """Every record Spark has stored (gpu_history)."""
    return _read_collection(COLLECTION_NAME, ["timestamp"], "timestamp")


@st.cache_data(ttl=30, show_spinner=False)
def query_aggregates():
    """Windowed aggregates computed by Spark (gpu_aggregates)."""
    return _read_collection(AGG_COLLECTION, ["window_start", "window_end"], "window_start")

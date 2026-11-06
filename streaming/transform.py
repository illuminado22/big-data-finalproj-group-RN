"""Spark transformations: parse Kafka messages and compute windowed aggregates."""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from streaming.config import WINDOW

MESSAGE_SCHEMA = StructType(
    [
        StructField("timestamp", StringType()),
        StructField("value", DoubleType()),
        StructField("metric_type", StringType()),
        StructField("sensor_id", StringType()),
        StructField("provider", StringType()),
        StructField("unit", StringType()),
    ]
)


def parse_stream(raw):
    """Kafka value (bytes) -> typed columns."""
    return (
        raw.select(F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("m"))
        .select("m.*")
        .withColumn("timestamp", F.col("timestamp").cast("timestamp"))
        .dropna(subset=["timestamp", "value", "sensor_id"])
    )


def aggregate(parsed):
    return (
        parsed.withWatermark("timestamp", "2 minutes")
        .dropDuplicates(["sensor_id", "provider", "timestamp"])  # replayed messages must not count twice
        .groupBy(F.window("timestamp", WINDOW), "sensor_id")
        .agg(
            F.avg("value").alias("avg_price"),
            F.min("value").alias("min_price"),
            F.max("value").alias("max_price"),
            F.stddev_pop("value").alias("std_price"),
            F.count("*").alias("samples"),
        )
    )

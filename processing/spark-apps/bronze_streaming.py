"""
Spark Structured Streaming job: reads from Kafka topics and writes to Iceberg bronze layer.
Run: docker exec spark-master /opt/spark/bin/spark-submit
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
     /opt/spark-apps/bronze_streaming.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, to_timestamp
)
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("BronzeStreaming") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n=== Starting Bronze Streaming Job ===\n")

KAFKA_BOOTSTRAP = "kafka:29092"

# ── Watch Events Schema ──────────────────────────────────────────────────────
watch_schema = StructType([
    StructField("event_id",               StringType(),  True),
    StructField("user_id",                StringType(),  True),
    StructField("content_id",             StringType(),  True),
    StructField("event_type",             StringType(),  True),
    StructField("device_type",            StringType(),  True),
    StructField("session_id",             StringType(),  True),
    StructField("watch_duration_seconds", IntegerType(), True),
    StructField("event_time",             StringType(),  True),
    StructField("ingestion_time",         StringType(),  True),
])

# ── Ratings Schema ───────────────────────────────────────────────────────────
ratings_schema = StructType([
    StructField("rating_id",      StringType(),  True),
    StructField("user_id",        StringType(),  True),
    StructField("content_id",     StringType(),  True),
    StructField("rating_value",   IntegerType(), True),
    StructField("event_time",     StringType(),  True),
    StructField("ingestion_time", StringType(),  True),
])

# ── Read watch-events from Kafka ─────────────────────────────────────────────
watch_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "watch-events") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

watch_df = watch_raw \
    .select(from_json(col("value").cast("string"), watch_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time",     to_timestamp(col("event_time"))) \
    .withColumn("ingestion_time", current_timestamp())

# ── Read ratings-late from Kafka ─────────────────────────────────────────────
ratings_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "ratings-late") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

ratings_df = ratings_raw \
    .select(from_json(col("value").cast("string"), ratings_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time",     to_timestamp(col("event_time"))) \
    .withColumn("ingestion_time", current_timestamp())

# ── Write watch-events to Iceberg bronze ─────────────────────────────────────
watch_query = watch_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .option("path", "local.bronze.watch_events") \
    .option("checkpointLocation", "/tmp/checkpoints/watch_events") \
    .start()

# ── Write ratings to Iceberg bronze ──────────────────────────────────────────
ratings_query = ratings_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .option("path", "local.bronze.ratings_late") \
    .option("checkpointLocation", "/tmp/checkpoints/ratings_late") \
    .start()

print("Streaming queries started:")
print("  watch-events  -> local.bronze.watch_events")
print("  ratings-late  -> local.bronze.ratings_late")
print("  Trigger: every 30 seconds")

spark.streams.awaitAnyTermination()

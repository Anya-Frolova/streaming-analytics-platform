"""
Spark Structured Streaming: Kafka -> Iceberg bronze.
Uses foreachBatch with direct writeTo.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_timestamp
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("BronzeStreaming") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== Starting Bronze Streaming Job ===\n")

KAFKA_BOOTSTRAP = "kafka:29092"

watch_schema = StructType([
    StructField("event_id",               StringType(),  True),
    StructField("user_id",                StringType(),  True),
    StructField("content_id",             StringType(),  True),
    StructField("event_type",             StringType(),  True),
    StructField("device_type",            StringType(),  True),
    StructField("session_id",             StringType(),  True),
    StructField("watch_duration_seconds", IntegerType(), True),
    StructField("event_time",             StringType(),  True),
])

ratings_schema = StructType([
    StructField("rating_id",    StringType(),  True),
    StructField("user_id",      StringType(),  True),
    StructField("content_id",   StringType(),  True),
    StructField("rating_value", IntegerType(), True),
    StructField("event_time",   StringType(),  True),
])

def write_watch_batch(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        print(f"  Batch {batch_id}: no watch events")
        return
    processed = batch_df \
        .withColumn("event_time",     to_timestamp(col("event_time"))) \
        .withColumn("ingestion_time", current_timestamp())
    processed.writeTo("local.bronze.watch_events").append()
    print(f"  Batch {batch_id}: wrote {count} watch events -> bronze.watch_events")

def write_ratings_batch(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        print(f"  Batch {batch_id}: no ratings")
        return
    processed = batch_df \
        .withColumn("event_time",     to_timestamp(col("event_time"))) \
        .withColumn("ingestion_time", current_timestamp())
    processed.writeTo("local.bronze.ratings_late").append()
    print(f"  Batch {batch_id}: wrote {count} ratings -> bronze.ratings_late")

watch_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "watch-events") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .select(from_json(col("value").cast("string"), watch_schema).alias("d")) \
    .select("d.*")

ratings_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "ratings-late") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .select(from_json(col("value").cast("string"), ratings_schema).alias("d")) \
    .select("d.*")

watch_query = watch_raw.writeStream \
    .foreachBatch(write_watch_batch) \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", "/tmp/checkpoints/watch_events") \
    .start()

ratings_query = ratings_raw.writeStream \
    .foreachBatch(write_ratings_batch) \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", "/tmp/checkpoints/ratings_late") \
    .start()

print("Streaming queries started:")
print("  watch-events  -> local.bronze.watch_events")
print("  ratings-late  -> local.bronze.ratings_late")
print("  Trigger: every 30 seconds")

spark.streams.awaitAnyTermination()

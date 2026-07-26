"""
Batch seed data loader: loads initial users and content catalog into Iceberg bronze layer.
Run: docker exec spark-master /opt/spark/bin/spark-submit /opt/spark-apps/bronze_seed_data.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import *
import random
from datetime import datetime, timezone, timedelta

spark = SparkSession.builder.appName("BronzeSeedData").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== Loading seed data into Bronze layer ===\n")

# ── 1. USERS ────────────────────────────────────────────────────────────────
users_schema = StructType([
    StructField("user_id",           StringType(),    False),
    StructField("username",          StringType(),    True),
    StructField("email",             StringType(),    True),
    StructField("age",               IntegerType(),   True),
    StructField("country",           StringType(),    True),
    StructField("subscription_tier", StringType(),    True),
    StructField("signup_date",       StringType(),    True),
    StructField("ingestion_time",    TimestampType(), True),
])

countries = ["US", "UK", "CA", "DE", "FR", "IL", "AU", "BR"]
tiers     = ["free", "basic", "standard", "premium"]

from datetime import datetime, timezone, timedelta
import random

now = datetime.now(timezone.utc)

users_data = []
for i in range(1, 101):
    signup = now - timedelta(days=random.randint(1, 730))
    users_data.append((
        f"user_{i:03d}",
        f"user_{i:03d}_name",
        f"user_{i:03d}@example.com",
        random.randint(18, 65),
        random.choice(countries),
        random.choice(tiers),
        signup.strftime("%Y-%m-%d"),
        now,
    ))

users_df = spark.createDataFrame(users_data, schema=users_schema)

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.bronze.users (
        user_id           STRING    NOT NULL,
        username          STRING,
        email             STRING,
        age               INT,
        country           STRING,
        subscription_tier STRING,
        signup_date       STRING,
        ingestion_time    TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

users_df.writeTo("local.bronze.users").append()
count = spark.table("local.bronze.users").count()
print(f"  OK: bronze.users — {count} rows")

# ── 2. CONTENT CATALOG ──────────────────────────────────────────────────────
content_schema = StructType([
    StructField("content_id",       StringType(),    False),
    StructField("title",            StringType(),    True),
    StructField("genre",            StringType(),    True),
    StructField("release_date",     StringType(),    True),
    StructField("duration_minutes", IntegerType(),   True),
    StructField("language",         StringType(),    True),
    StructField("ingestion_time",   TimestampType(), True),
])

genres    = ["Drama", "Comedy", "Action", "Thriller", "Documentary", "Sci-Fi", "Romance", "Horror"]
languages = ["EN", "ES", "FR", "DE", "HE", "PT"]

content_data = []
for i in range(1, 51):
    release = now - timedelta(days=random.randint(30, 3650))
    content_data.append((
        f"content_{i:03d}",
        f"Title {i}",
        random.choice(genres),
        release.strftime("%Y-%m-%d"),
        random.randint(20, 180),
        random.choice(languages),
        now,
    ))

content_df = spark.createDataFrame(content_data, schema=content_schema)

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.bronze.content_catalog (
        content_id       STRING    NOT NULL,
        title            STRING,
        genre            STRING,
        release_date     STRING,
        duration_minutes INT,
        language         STRING,
        ingestion_time   TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

content_df.writeTo("local.bronze.content_catalog").append()
count = spark.table("local.bronze.content_catalog").count()
print(f"  OK: bronze.content_catalog — {count} rows")

# ── 3. WATCH EVENTS bronze table (schema only) ───────────────────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.bronze.watch_events (
        event_id               STRING,
        user_id                STRING,
        content_id             STRING,
        event_type             STRING,
        device_type            STRING,
        session_id             STRING,
        watch_duration_seconds INT,
        event_time             TIMESTAMP,
        ingestion_time         TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")
print("  OK: bronze.watch_events table created")

# ── 4. RATINGS LATE bronze table (schema only) ────────────────────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.bronze.ratings_late (
        rating_id      STRING,
        user_id        STRING,
        content_id     STRING,
        rating_value   INT,
        event_time     TIMESTAMP,
        ingestion_time TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")
print("  OK: bronze.ratings_late table created")

print("\nSUCCESS: Bronze seed data loaded\n")
spark.stop()

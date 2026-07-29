from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, timezone, timedelta
import random, uuid

spark = SparkSession.builder.appName("BronzeSeedData").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("\n=== Loading seed data into Bronze layer ===\n")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")

now = datetime.now(timezone.utc)
countries   = ["US","UK","CA","DE","FR","IL","AU","BR"]
tiers       = ["free","basic","standard","premium"]
genres      = ["Drama","Comedy","Action","Thriller","Documentary","Sci-Fi","Romance","Horror"]
languages   = ["EN","ES","FR","DE","HE","PT"]
event_types = ["play","pause","stop","finish"]
devices     = ["mobile","desktop","tablet","smart_tv"]

# 1. USERS
spark.sql("""CREATE TABLE IF NOT EXISTS local.bronze.users (
    user_id STRING NOT NULL, username STRING, email STRING, age INT,
    country STRING, subscription_tier STRING, signup_date STRING,
    ingestion_time TIMESTAMP
) USING iceberg TBLPROPERTIES ('format-version'='2')""")

if spark.table("local.bronze.users").count() == 0:
    data = [(f"user_{i:03d}", f"user_{i:03d}_name", f"user_{i:03d}@example.com",
             random.randint(18,65), random.choice(countries), random.choice(tiers),
             (now-timedelta(days=random.randint(1,730))).strftime("%Y-%m-%d"), now)
            for i in range(1,101)]
    spark.createDataFrame(data, StructType([
        StructField("user_id",StringType(),False),
        StructField("username",StringType(),True),
        StructField("email",StringType(),True),
        StructField("age",IntegerType(),True),
        StructField("country",StringType(),True),
        StructField("subscription_tier",StringType(),True),
        StructField("signup_date",StringType(),True),
        StructField("ingestion_time",TimestampType(),True)
    ])).writeTo("local.bronze.users").append()
    print(f"  OK: bronze.users — 100 rows inserted")
else:
    print(f"  OK: bronze.users — already has data, skipping")
print(f"  Total: {spark.table('local.bronze.users').count()} rows")

# 2. CONTENT CATALOG
spark.sql("""CREATE TABLE IF NOT EXISTS local.bronze.content_catalog (
    content_id STRING NOT NULL, title STRING, genre STRING,
    release_date STRING, duration_minutes INT, language STRING,
    ingestion_time TIMESTAMP
) USING iceberg TBLPROPERTIES ('format-version'='2')""")

if spark.table("local.bronze.content_catalog").count() == 0:
    data = [(f"content_{i:03d}", f"Title {i}", random.choice(genres),
             (now-timedelta(days=random.randint(30,3650))).strftime("%Y-%m-%d"),
             random.randint(20,180), random.choice(languages), now)
            for i in range(1,51)]
    spark.createDataFrame(data, StructType([
        StructField("content_id",StringType(),False),
        StructField("title",StringType(),True),
        StructField("genre",StringType(),True),
        StructField("release_date",StringType(),True),
        StructField("duration_minutes",IntegerType(),True),
        StructField("language",StringType(),True),
        StructField("ingestion_time",TimestampType(),True)
    ])).writeTo("local.bronze.content_catalog").append()
    print(f"  OK: bronze.content_catalog — 50 rows inserted")
else:
    print(f"  OK: bronze.content_catalog — already has data, skipping")
print(f"  Total: {spark.table('local.bronze.content_catalog').count()} rows")

# 3. WATCH EVENTS
spark.sql("""CREATE TABLE IF NOT EXISTS local.bronze.watch_events (
    event_id STRING, user_id STRING, content_id STRING,
    event_type STRING, device_type STRING, session_id STRING,
    watch_duration_seconds INT, event_time TIMESTAMP,
    ingestion_time TIMESTAMP
) USING iceberg TBLPROPERTIES ('format-version'='2')""")

if spark.table("local.bronze.watch_events").count() == 0:
    data = [(str(uuid.uuid4()), f"user_{random.randint(1,100):03d}",
             f"content_{random.randint(1,50):03d}", random.choice(event_types),
             random.choice(devices), str(uuid.uuid4()), random.randint(30,7200),
             now-timedelta(hours=random.uniform(0,72)), now)
            for i in range(1000)]
    spark.createDataFrame(data, StructType([
        StructField("event_id",StringType(),False),
        StructField("user_id",StringType(),True),
        StructField("content_id",StringType(),True),
        StructField("event_type",StringType(),True),
        StructField("device_type",StringType(),True),
        StructField("session_id",StringType(),True),
        StructField("watch_duration_seconds",IntegerType(),True),
        StructField("event_time",TimestampType(),True),
        StructField("ingestion_time",TimestampType(),True)
    ])).writeTo("local.bronze.watch_events").append()
    print(f"  OK: bronze.watch_events — 1000 rows inserted")
else:
    print(f"  OK: bronze.watch_events — already has data, skipping")
print(f"  Total: {spark.table('local.bronze.watch_events').count()} rows")

# 4. RATINGS LATE
spark.sql("""CREATE TABLE IF NOT EXISTS local.bronze.ratings_late (
    rating_id STRING, user_id STRING, content_id STRING,
    rating_value INT, event_time TIMESTAMP, ingestion_time TIMESTAMP
) USING iceberg TBLPROPERTIES ('format-version'='2')""")

if spark.table("local.bronze.ratings_late").count() == 0:
    data = [(str(uuid.uuid4()), f"user_{random.randint(1,100):03d}",
             f"content_{random.randint(1,50):03d}", random.randint(1,5),
             now-timedelta(hours=random.uniform(1,48)), now)
            for i in range(300)]
    spark.createDataFrame(data, StructType([
        StructField("rating_id",StringType(),False),
        StructField("user_id",StringType(),True),
        StructField("content_id",StringType(),True),
        StructField("rating_value",IntegerType(),True),
        StructField("event_time",TimestampType(),True),
        StructField("ingestion_time",TimestampType(),True)
    ])).writeTo("local.bronze.ratings_late").append()
    print(f"  OK: bronze.ratings_late — 300 rows inserted")
else:
    print(f"  OK: bronze.ratings_late — already has data, skipping")
print(f"  Total: {spark.table('local.bronze.ratings_late').count()} rows")

print("\nSUCCESS: Bronze seed data loaded\n")
spark.stop()

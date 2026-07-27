"""
Bronze -> Silver ETL job.
Implements:
- Watch sessions aggregation (bronze -> silver)
- SCD Type 2 for dim_user
- Late-arriving data handling (48h window)
- Data quality checks
Run: docker exec spark-master /opt/spark/bin/spark-submit
     --master spark://spark-master:7077
     /opt/spark-apps/silver_etl.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, when, max as spark_max,
    min as spark_min, sum as spark_sum, avg, count,
    round as spark_round, datediff, to_date, expr,
    row_number, md5, concat_ws
)
from pyspark.sql.window import Window
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("SilverETL") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== Bronze -> Silver ETL ===\n")

now = current_timestamp()

# ── CREATE SILVER NAMESPACE ──────────────────────────────────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")

# ════════════════════════════════════════════════════════════════════════════
# 1. DATA QUALITY CHECKS on bronze
# ════════════════════════════════════════════════════════════════════════════
print("--- Data Quality Checks ---")

bronze_users = spark.table("local.bronze.users")
total = bronze_users.count()
nulls = bronze_users.filter(col("user_id").isNull()).count()
dups  = total - bronze_users.select("user_id").distinct().count()
print(f"  bronze.users: {total} rows | null user_id: {nulls} | duplicates: {dups}")
assert nulls == 0, "FAIL: null user_ids in bronze.users"
assert dups  == 0, "FAIL: duplicate user_ids in bronze.users"
print("  OK: bronze.users quality checks passed")

bronze_content = spark.table("local.bronze.content_catalog")
total_c = bronze_content.count()
nulls_c = bronze_content.filter(col("content_id").isNull()).count()
print(f"  bronze.content_catalog: {total_c} rows | null content_id: {nulls_c}")
assert nulls_c == 0, "FAIL: null content_ids"
print("  OK: bronze.content_catalog quality checks passed")

# ════════════════════════════════════════════════════════════════════════════
# 2. SCD TYPE 2 — dim_user
# ════════════════════════════════════════════════════════════════════════════
print("\n--- SCD Type 2: dim_user ---")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.silver.dim_user (
        user_key          BIGINT,
        user_id           STRING    NOT NULL,
        username          STRING,
        email             STRING,
        age               INT,
        age_band          STRING,
        country           STRING,
        subscription_tier STRING,
        signup_date       STRING,
        effective_from    DATE      NOT NULL,
        effective_to      DATE,
        is_current        BOOLEAN   NOT NULL,
        ingestion_time    TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

existing = spark.table("local.silver.dim_user")
existing_count = existing.count()

bronze_users_prep = bronze_users \
    .withColumn("age_band", when(col("age") < 25, "18-24")
        .when(col("age") < 35, "25-34")
        .when(col("age") < 45, "35-44")
        .when(col("age") < 55, "45-54")
        .otherwise("55+")) \
    .withColumn("row_hash", md5(concat_ws("|",
        col("subscription_tier"), col("country"), col("email"))))

if existing_count == 0:
    # First load - insert all as current
    users_scd = bronze_users_prep \
        .withColumn("user_key",       expr("monotonically_increasing_id()")) \
        .withColumn("effective_from", to_date(lit("2020-01-01"))) \
        .withColumn("effective_to",   lit(None).cast(DateType())) \
        .withColumn("is_current",     lit(True)) \
        .withColumn("ingestion_time", current_timestamp()) \
        .select("user_key", "user_id", "username", "email", "age", "age_band",
                "country", "subscription_tier", "signup_date",
                "effective_from", "effective_to", "is_current", "ingestion_time")
    users_scd.writeTo("local.silver.dim_user").append()
    print(f"  OK: initial load — {users_scd.count()} users inserted as current")
else:
    # SCD2 merge: detect changed records
    current_records = existing.filter(col("is_current") == True)
    joined = bronze_users_prep.alias("new").join(
        current_records.alias("old"), "user_id", "left"
    )
    changed = joined.filter(
        (col("new.subscription_tier") != col("old.subscription_tier")) |
        (col("new.country")           != col("old.country")) |
        col("old.user_id").isNull()
    )
    changed_count = changed.count()
    if changed_count > 0:
        today = to_date(current_timestamp())
        # Expire old records
        expired = current_records.join(
            changed.select(col("new.user_id")), "user_id"
        ).withColumn("effective_to", today) \
         .withColumn("is_current",   lit(False))
        expired.writeTo("local.silver.dim_user").overwritePartitions()
        # Insert new versions
        max_key = existing.agg(spark_max("user_key")).collect()[0][0] or 0
        new_versions = changed \
            .withColumn("user_key",       expr(f"monotonically_increasing_id() + {max_key} + 1")) \
            .withColumn("effective_from", today) \
            .withColumn("effective_to",   lit(None).cast(DateType())) \
            .withColumn("is_current",     lit(True)) \
            .withColumn("ingestion_time", current_timestamp()) \
            .select(col("new.user_id"), col("new.username"), col("new.email"), col("new.age"), col("new.age_band"), col("new.country"), col("new.subscription_tier"), col("new.signup_date"), "user_key", "effective_from", "effective_to", "is_current").withColumn("ingestion_time", current_timestamp())
        new_versions.writeTo("local.silver.dim_user").append()
        print(f"  OK: SCD2 — {changed_count} records updated")
    else:
        print("  OK: no changes detected in dim_user")

print(f"  dim_user total: {spark.table('local.silver.dim_user').count()} rows")

# ════════════════════════════════════════════════════════════════════════════
# 3. dim_content
# ════════════════════════════════════════════════════════════════════════════
print("\n--- dim_content ---")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.silver.dim_content (
        content_id       STRING NOT NULL,
        title            STRING,
        genre            STRING,
        release_date     STRING,
        duration_minutes INT,
        language         STRING,
        ingestion_time   TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

bronze_content \
    .withColumn("ingestion_time", current_timestamp()) \
    .writeTo("local.silver.dim_content").overwritePartitions()

print(f"  OK: dim_content — {spark.table('local.silver.dim_content').count()} rows")

# ════════════════════════════════════════════════════════════════════════════
# 4. watch_sessions (bronze -> silver) with LATE-ARRIVING DATA handling
# ════════════════════════════════════════════════════════════════════════════
print("\n--- watch_sessions with late-arriving data ---")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.silver.watch_sessions (
        session_id             STRING,
        user_id                STRING,
        content_id             STRING,
        event_type             STRING,
        device_type            STRING,
        watch_duration_seconds INT,
        completion_percent     DOUBLE,
        event_time             TIMESTAMP,
        ingestion_time         TIMESTAMP,
        is_late_arrival        BOOLEAN,
        processing_date        DATE
    ) USING iceberg
    TBLPROPERTIES (
        'format-version'                 = '2',
        'write.target-file-size-bytes'   = '134217728'
    )
""")

watch_bronze = spark.table("local.bronze.watch_events")
watch_count  = watch_bronze.count()

if watch_count > 0:
    # Late-arriving: event_time is more than 5 min but less than 48h ago
    watch_silver = watch_bronze \
        .withColumn("is_late_arrival",
            (col("ingestion_time").cast("long") - col("event_time").cast("long") > 300) &
            (col("ingestion_time").cast("long") - col("event_time").cast("long") < 172800)
        ) \
        .withColumn("completion_percent",
            spark_round(col("watch_duration_seconds") / lit(3600) * 100, 2)) \
        .withColumn("processing_date", to_date(current_timestamp())) \
        .withColumn("ingestion_time",  current_timestamp()) \
        .select(
            col("session_id"),
            col("user_id"),
            col("content_id"),
            col("event_type"),
            col("device_type"),
            col("watch_duration_seconds"),
            col("completion_percent"),
            col("event_time"),
            col("ingestion_time"),
            col("is_late_arrival"),
            col("processing_date")
        )
    watch_silver.writeTo("local.silver.watch_sessions").append()
    late_count = watch_silver.filter(col("is_late_arrival") == True).count()
    print(f"  OK: watch_sessions — {watch_silver.count()} rows ({late_count} late arrivals)")
else:
    print("  INFO: no watch_events yet — table is empty, schema ready")

# ════════════════════════════════════════════════════════════════════════════
# 5. ratings_late silver (with 48h window filter)
# ════════════════════════════════════════════════════════════════════════════
print("\n--- ratings_late silver ---")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.silver.ratings (
        rating_id      STRING,
        user_id        STRING,
        content_id     STRING,
        rating_value   INT,
        event_time     TIMESTAMP,
        ingestion_time TIMESTAMP,
        is_within_48h  BOOLEAN
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

ratings_bronze = spark.table("local.bronze.ratings_late")
ratings_count  = ratings_bronze.count()

if ratings_count > 0:
    ratings_silver = ratings_bronze \
        .withColumn("is_within_48h",
            col("ingestion_time").cast("long") - col("event_time").cast("long") < 172800
        ) \
        .withColumn("ingestion_time", current_timestamp())
    ratings_silver.writeTo("local.silver.ratings").append()
    print(f"  OK: ratings — {ratings_silver.count()} rows")
else:
    print("  INFO: no ratings yet — table is empty, schema ready")

print("\nSUCCESS: Bronze -> Silver ETL completed\n")
spark.stop()


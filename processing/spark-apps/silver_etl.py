from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, when,
    sum as spark_sum, avg, count, round as spark_round,
    to_date, expr, md5, concat_ws
)
from pyspark.sql.types import DateType

spark = SparkSession.builder.appName("SilverETL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("\n=== Bronze -> Silver ETL ===\n")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")

# ── DATA QUALITY CHECKS ──────────────────────────────────────────────────
print("--- Data Quality Checks ---")
bronze_users = spark.table("local.bronze.users")
total = bronze_users.count()
nulls = bronze_users.filter(col("user_id").isNull()).count()
dups  = total - bronze_users.select("user_id").distinct().count()
print(f"  bronze.users: {total} rows | null user_id: {nulls} | duplicates: {dups}")
assert nulls == 0, "FAIL: null user_ids"
assert dups  == 0, "FAIL: duplicate user_ids"
print("  OK: bronze.users quality checks passed")

bronze_content = spark.table("local.bronze.content_catalog")
nulls_c = bronze_content.filter(col("content_id").isNull()).count()
print(f"  bronze.content_catalog: {bronze_content.count()} rows | null content_id: {nulls_c}")
assert nulls_c == 0, "FAIL: null content_ids"
print("  OK: bronze.content_catalog quality checks passed")

# ── SCD TYPE 2 — dim_user ────────────────────────────────────────────────
print("\n--- SCD Type 2: dim_user ---")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.silver.dim_user (
        user_key          BIGINT,
        user_id           STRING NOT NULL,
        username          STRING,
        email             STRING,
        age               INT,
        age_band          STRING,
        country           STRING,
        subscription_tier STRING,
        signup_date       STRING,
        effective_from    DATE NOT NULL,
        effective_to      DATE,
        is_current        BOOLEAN NOT NULL,
        ingestion_time    TIMESTAMP
    ) USING iceberg TBLPROPERTIES ('format-version'='2')
""")

existing_count = spark.table("local.silver.dim_user").count()
bronze_prep = bronze_users \
    .withColumn("age_band", when(col("age") < 25, "18-24")
        .when(col("age") < 35, "25-34")
        .when(col("age") < 45, "35-44")
        .when(col("age") < 55, "45-54")
        .otherwise("55+"))

if existing_count == 0:
    users_scd = bronze_prep \
        .withColumn("user_key",       expr("monotonically_increasing_id()")) \
        .withColumn("effective_from", to_date(lit("2020-01-01"))) \
        .withColumn("effective_to",   lit(None).cast(DateType())) \
        .withColumn("is_current",     lit(True)) \
        .withColumn("ingestion_time", current_timestamp()) \
        .select("user_key", "user_id", "username", "email", "age", "age_band",
                "country", "subscription_tier", "signup_date",
                "effective_from", "effective_to", "is_current", "ingestion_time")
    users_scd.writeTo("local.silver.dim_user").append()
    print(f"  OK: initial load — {spark.table('local.silver.dim_user').count()} users")
else:
    print(f"  OK: dim_user already has {existing_count} rows")

# ── dim_content ──────────────────────────────────────────────────────────
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
    ) USING iceberg TBLPROPERTIES ('format-version'='2')
""")
if spark.table("local.silver.dim_content").count() == 0:
    bronze_content \
        .withColumn("ingestion_time", current_timestamp()) \
        .writeTo("local.silver.dim_content").append()
print(f"  OK: dim_content — {spark.table('local.silver.dim_content').count()} rows")

# ── watch_sessions (LATE-ARRIVING DATA) ──────────────────────────────────
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
    ) USING iceberg TBLPROPERTIES ('format-version'='2')
""")

if spark.table("local.silver.watch_sessions").count() == 0:
    watch_bronze = spark.table("local.bronze.watch_events")
    watch_count  = watch_bronze.count()
    if watch_count > 0:
        watch_silver = watch_bronze \
            .withColumn("is_late_arrival",
                (col("ingestion_time").cast("long") - col("event_time").cast("long") > 300) &
                (col("ingestion_time").cast("long") - col("event_time").cast("long") < 172800)
            ) \
            .withColumn("completion_percent",
                spark_round(col("watch_duration_seconds") / lit(3600) * 100, 2)) \
            .withColumn("processing_date",    to_date(current_timestamp())) \
            .withColumn("ingestion_time",     current_timestamp()) \
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
        late = watch_silver.filter(col("is_late_arrival") == True).count()
        print(f"  OK: watch_sessions — {spark.table('local.silver.watch_sessions').count()} rows ({late} late arrivals)")
    else:
        print("  INFO: no watch_events yet")
else:
    print(f"  OK: watch_sessions already has {spark.table('local.silver.watch_sessions').count()} rows")

# ── ratings ───────────────────────────────────────────────────────────────
print("\n--- ratings ---")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.silver.ratings (
        rating_id      STRING,
        user_id        STRING,
        content_id     STRING,
        rating_value   INT,
        event_time     TIMESTAMP,
        ingestion_time TIMESTAMP,
        is_within_48h  BOOLEAN
    ) USING iceberg TBLPROPERTIES ('format-version'='2')
""")
if spark.table("local.silver.ratings").count() == 0:
    ratings_bronze = spark.table("local.bronze.ratings_late")
    if ratings_bronze.count() > 0:
        ratings_silver = ratings_bronze \
            .withColumn("is_within_48h",
                col("ingestion_time").cast("long") - col("event_time").cast("long") < 172800) \
            .withColumn("ingestion_time", current_timestamp()) \
            .select("rating_id", "user_id", "content_id", "rating_value",
                    "event_time", "ingestion_time", "is_within_48h")
        ratings_silver.writeTo("local.silver.ratings").append()
        print(f"  OK: ratings — {spark.table('local.silver.ratings').count()} rows")
    else:
        print("  INFO: no ratings yet")
else:
    print(f"  OK: ratings already has {spark.table('local.silver.ratings').count()} rows")

print("\nSUCCESS: Bronze -> Silver ETL completed\n")
spark.stop()

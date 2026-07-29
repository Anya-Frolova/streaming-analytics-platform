from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, when, max as spark_max,
    sum as spark_sum, avg, count, round as spark_round,
    to_date, expr, dayofweek, weekofyear, quarter,
    month, year, dayofmonth
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
assert nulls == 0, "FAIL: null user_ids in bronze.users"
assert dups  == 0, "FAIL: duplicate user_ids in bronze.users"
print("  OK: bronze.users quality checks passed")

bronze_content = spark.table("local.bronze.content_catalog")
nulls_c = bronze_content.filter(col("content_id").isNull()).count()
print(f"  bronze.content_catalog: {bronze_content.count()} rows | null content_id: {nulls_c}")
assert nulls_c == 0, "FAIL: null content_ids"
print("  OK: bronze.content_catalog quality checks passed")

watch_bronze = spark.table("local.bronze.watch_events")
watch_nulls = watch_bronze.filter(col("event_id").isNull()).count()
print(f"  bronze.watch_events: {watch_bronze.count()} rows | null event_id: {watch_nulls}")
assert watch_nulls == 0, "FAIL: null event_ids"
print("  OK: bronze.watch_events quality checks passed")

# ── SCD TYPE 2 — dim_user (REAL MERGE LOGIC) ─────────────────────────────
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

bronze_prep = bronze_users \
    .withColumn("age_band", when(col("age") < 25, "18-24")
        .when(col("age") < 35, "25-34")
        .when(col("age") < 45, "35-44")
        .when(col("age") < 55, "45-54")
        .otherwise("55+"))

existing_count = spark.table("local.silver.dim_user").count()

if existing_count == 0:
    # Initial load
    users_scd = bronze_prep \
        .withColumn("user_key",       expr("monotonically_increasing_id()")) \
        .withColumn("effective_from", to_date(lit("2020-01-01"))) \
        .withColumn("effective_to",   lit(None).cast(DateType())) \
        .withColumn("is_current",     lit(True)) \
        .withColumn("ingestion_time", current_timestamp()) \
        .select("user_key","user_id","username","email","age","age_band",
                "country","subscription_tier","signup_date",
                "effective_from","effective_to","is_current","ingestion_time")
    users_scd.writeTo("local.silver.dim_user").append()
    print(f"  OK: initial load — {spark.table('local.silver.dim_user').count()} users")
else:
    # SCD2 MERGE: detect changed records
    current_records = spark.table("local.silver.dim_user").filter(col("is_current") == True)
    today = to_date(current_timestamp())

    # Find users whose subscription_tier or country changed
    joined = bronze_prep.alias("new") \
        .join(current_records.alias("old"), "user_id", "inner")

    changed = joined.filter(
        (col("new.subscription_tier") != col("old.subscription_tier")) |
        (col("new.country") != col("old.country"))
    )
    changed_count = changed.count()

    if changed_count > 0:
        print(f"  Detected {changed_count} changed users — applying SCD2...")

        # Step 1: expire old records by ingesting updated versions
        changed_user_ids = [row["user_id"] for row in changed.select("user_id").collect()]

        # Get all current records for changed users and expire them
        to_expire = current_records.filter(col("user_id").isin(changed_user_ids)) \
            .withColumn("effective_to", today) \
            .withColumn("is_current",   lit(False)) \
            .withColumn("ingestion_time", current_timestamp())

        to_expire.writeTo("local.silver.dim_user").overwritePartitions()

        # Step 2: insert new versions
        max_key = spark.table("local.silver.dim_user") \
            .agg(spark_max("user_key")).collect()[0][0] or 0

        new_versions = bronze_prep \
            .filter(col("user_id").isin(changed_user_ids)) \
            .withColumn("user_key",       expr(f"monotonically_increasing_id() + {max_key} + 1")) \
            .withColumn("effective_from", today) \
            .withColumn("effective_to",   lit(None).cast(DateType())) \
            .withColumn("is_current",     lit(True)) \
            .withColumn("ingestion_time", current_timestamp()) \
            .select("user_key","user_id","username","email","age","age_band",
                    "country","subscription_tier","signup_date",
                    "effective_from","effective_to","is_current","ingestion_time")
        new_versions.writeTo("local.silver.dim_user").append()
        print(f"  OK: SCD2 — {changed_count} records updated")
    else:
        print("  OK: no changes detected in dim_user")

print(f"  dim_user total: {spark.table('local.silver.dim_user').count()} rows")

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
    bronze_content.withColumn("ingestion_time", current_timestamp()) \
        .writeTo("local.silver.dim_content").append()
print(f"  OK: dim_content — {spark.table('local.silver.dim_content').count()} rows")

# ── dim_time ─────────────────────────────────────────────────────────────
print("\n--- dim_time ---")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.silver.dim_time (
        date_key      DATE NOT NULL,
        year          INT,
        quarter       INT,
        month         INT,
        day           INT,
        day_of_week   INT,
        week_of_year  INT,
        ingestion_time TIMESTAMP
    ) USING iceberg TBLPROPERTIES ('format-version'='2')
""")
existing_dates = spark.table("local.silver.dim_time").count()
new_dates = watch_bronze.select(to_date(col("event_time")).alias("date_key")).distinct()
if existing_dates == 0:
    new_dates \
        .withColumn("year",          year(col("date_key"))) \
        .withColumn("quarter",       quarter(col("date_key"))) \
        .withColumn("month",         month(col("date_key"))) \
        .withColumn("day",           dayofmonth(col("date_key"))) \
        .withColumn("day_of_week",   dayofweek(col("date_key"))) \
        .withColumn("week_of_year",  weekofyear(col("date_key"))) \
        .withColumn("ingestion_time", current_timestamp()) \
        .writeTo("local.silver.dim_time").append()
print(f"  OK: dim_time — {spark.table('local.silver.dim_time').count()} rows")

# ── dim_device ────────────────────────────────────────────────────────────
print("\n--- dim_device ---")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.silver.dim_device (
        device_type     STRING NOT NULL,
        device_category STRING,
        ingestion_time  TIMESTAMP
    ) USING iceberg TBLPROPERTIES ('format-version'='2')
""")
if spark.table("local.silver.dim_device").count() == 0:
    watch_bronze.select(col("device_type")).distinct() \
        .withColumn("device_category",
            when(col("device_type") == "smart_tv", "TV")
            .when(col("device_type") == "desktop",  "Computer")
            .when(col("device_type") == "mobile",   "Mobile")
            .otherwise("Tablet")) \
        .withColumn("ingestion_time", current_timestamp()) \
        .writeTo("local.silver.dim_device").append()
print(f"  OK: dim_device — {spark.table('local.silver.dim_device').count()} rows")

# ── watch_sessions — INCREMENTAL + completion_percent fix ─────────────────
print("\n--- watch_sessions (incremental, late-arriving data) ---")
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

# Incremental: only process events not yet in silver
existing_sessions = spark.table("local.silver.watch_sessions")
if existing_sessions.count() == 0:
    max_ingestion = None
else:
    max_ingestion = existing_sessions.agg({"ingestion_time": "max"}).collect()[0][0]

if max_ingestion is None:
    new_watch = watch_bronze
else:
    new_watch = watch_bronze.filter(col("ingestion_time") > lit(max_ingestion))

new_count = new_watch.count()
if new_count > 0:
    # FIX: completion_percent using duration_minutes from dim_content
    dim_content = spark.table("local.silver.dim_content")
    watch_silver = new_watch \
        .join(dim_content.select("content_id", "duration_minutes"), "content_id", "left") \
        .withColumn("is_late_arrival",
            (col("ingestion_time").cast("long") - col("event_time").cast("long") > 300) &
            (col("ingestion_time").cast("long") - col("event_time").cast("long") < 172800)) \
        .withColumn("completion_percent",
            when(col("duration_minutes").isNotNull(),
                spark_round(col("watch_duration_seconds") /
                    (col("duration_minutes") * 60) * 100, 2))
            .otherwise(
                spark_round(col("watch_duration_seconds") / lit(3600) * 100, 2))) \
        .withColumn("processing_date",    to_date(current_timestamp())) \
        .withColumn("ingestion_time",     current_timestamp()) \
        .select("session_id","user_id","content_id","event_type","device_type",
                "watch_duration_seconds","completion_percent","event_time",
                "ingestion_time","is_late_arrival","processing_date")
    watch_silver.writeTo("local.silver.watch_sessions").append()
    late = watch_silver.filter(col("is_late_arrival") == True).count()
    print(f"  OK: added {new_count} new watch_sessions ({late} late arrivals)")
else:
    print(f"  OK: no new watch_events since last run")
print(f"  watch_sessions total: {spark.table('local.silver.watch_sessions').count()} rows")

# ── ratings — INCREMENTAL ─────────────────────────────────────────────────
print("\n--- ratings (incremental) ---")
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
existing_ratings = spark.table("local.silver.ratings")
if existing_ratings.count() == 0:
    max_r_ingestion = None
else:
    max_r_ingestion = existing_ratings.agg({"ingestion_time": "max"}).collect()[0][0]

ratings_bronze = spark.table("local.bronze.ratings_late")
if max_r_ingestion is None:
    new_ratings = ratings_bronze
else:
    new_ratings = ratings_bronze.filter(col("ingestion_time") > lit(max_r_ingestion))

new_r_count = new_ratings.count()
if new_r_count > 0:
    new_ratings \
        .withColumn("is_within_48h",
            col("ingestion_time").cast("long") - col("event_time").cast("long") < 172800) \
        .withColumn("ingestion_time", current_timestamp()) \
        .select("rating_id","user_id","content_id","rating_value",
                "event_time","ingestion_time","is_within_48h") \
        .writeTo("local.silver.ratings").append()
    print(f"  OK: added {new_r_count} new ratings")
else:
    print(f"  OK: no new ratings since last run")
print(f"  ratings total: {spark.table('local.silver.ratings').count()} rows")

print("\nSUCCESS: Bronze -> Silver ETL completed\n")
spark.stop()

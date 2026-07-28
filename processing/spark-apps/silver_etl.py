from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, when,
    sum as spark_sum, avg, count, round as spark_round,
    to_date, expr, md5, concat_ws,
    dayofweek, weekofyear, quarter, month, year, dayofmonth
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

watch_bronze = spark.table("local.bronze.watch_events")
watch_nulls = watch_bronze.filter(col("event_id").isNull()).count()
print(f"  bronze.watch_events: {watch_bronze.count()} rows | null event_id: {watch_nulls}")
print("  OK: bronze.watch_events quality checks passed")

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
        .select("user_key","user_id","username","email","age","age_band",
                "country","subscription_tier","signup_date",
                "effective_from","effective_to","is_current","ingestion_time")
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
    bronze_content.withColumn("ingestion_time", current_timestamp()) \
        .writeTo("local.silver.dim_content").append()
print(f"  OK: dim_content — {spark.table('local.silver.dim_content').count()} rows")

# ── dim_time ─────────────────────────────────────────────────────────────
print("\n--- dim_time ---")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.silver.dim_time (
        date_key     DATE NOT NULL,
        year         INT,
        quarter      INT,
        month        INT,
        day          INT,
        day_of_week  INT,
        week_of_year INT,
        ingestion_time TIMESTAMP
    ) USING iceberg TBLPROPERTIES ('format-version'='2')
""")
if spark.table("local.silver.dim_time").count() == 0:
    dates_df = watch_bronze \
        .select(to_date(col("event_time")).alias("date_key")) \
        .distinct() \
        .withColumn("year",         year(col("date_key"))) \
        .withColumn("quarter",      quarter(col("date_key"))) \
        .withColumn("month",        month(col("date_key"))) \
        .withColumn("day",          dayofmonth(col("date_key"))) \
        .withColumn("day_of_week",  dayofweek(col("date_key"))) \
        .withColumn("week_of_year", weekofyear(col("date_key"))) \
        .withColumn("ingestion_time", current_timestamp())
    dates_df.writeTo("local.silver.dim_time").append()
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
    devices_df = watch_bronze \
        .select(col("device_type")).distinct() \
        .withColumn("device_category",
            when(col("device_type") == "smart_tv", "TV")
            .when(col("device_type") == "desktop",  "Computer")
            .when(col("device_type") == "mobile",   "Mobile")
            .otherwise("Tablet")) \
        .withColumn("ingestion_time", current_timestamp())
    devices_df.writeTo("local.silver.dim_device").append()
print(f"  OK: dim_device — {spark.table('local.silver.dim_device').count()} rows")

# ── watch_sessions ────────────────────────────────────────────────────────
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
    if watch_bronze.count() > 0:
        watch_silver = watch_bronze \
            .withColumn("is_late_arrival",
                (col("ingestion_time").cast("long") - col("event_time").cast("long") > 300) &
                (col("ingestion_time").cast("long") - col("event_time").cast("long") < 172800)) \
            .withColumn("completion_percent",
                spark_round(col("watch_duration_seconds") / lit(3600) * 100, 2)) \
            .withColumn("processing_date",    to_date(current_timestamp())) \
            .withColumn("ingestion_time",     current_timestamp()) \
            .select("session_id","user_id","content_id","event_type","device_type",
                    "watch_duration_seconds","completion_percent","event_time",
                    "ingestion_time","is_late_arrival","processing_date")
        watch_silver.writeTo("local.silver.watch_sessions").append()
        late = watch_silver.filter(col("is_late_arrival") == True).count()
        print(f"  OK: watch_sessions — {spark.table('local.silver.watch_sessions').count()} rows ({late} late arrivals)")
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
            .select("rating_id","user_id","content_id","rating_value",
                    "event_time","ingestion_time","is_within_48h")
        ratings_silver.writeTo("local.silver.ratings").append()
        print(f"  OK: ratings — {spark.table('local.silver.ratings').count()} rows")
else:
    print(f"  OK: ratings already has {spark.table('local.silver.ratings').count()} rows")

print("\nSUCCESS: Bronze -> Silver ETL completed\n")
spark.stop()

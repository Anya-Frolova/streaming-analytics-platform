"""
Silver -> Gold ETL job.
Creates:
- fact_watch_sessions (star schema fact table)
- daily_engagement (dashboard table, per date)
- churn_features (ML feature table, per user per day)
Run: docker exec spark-master /opt/spark/bin/spark-submit
     --master spark://spark-master:7077
     /opt/spark-apps/gold_etl.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, when,
    sum as spark_sum, avg, count, countDistinct,
    round as spark_round, datediff, to_date,
    max as spark_max, min as spark_min,
    expr, date_sub
)
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("GoldETL") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== Silver -> Gold ETL ===\n")

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")

# ════════════════════════════════════════════════════════════════════════════
# 1. fact_watch_sessions (Star Schema fact table)
# ════════════════════════════════════════════════════════════════════════════
print("--- fact_watch_sessions ---")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.gold.fact_watch_sessions (
        session_id             STRING,
        user_key               BIGINT,
        user_id                STRING,
        content_id             STRING,
        event_type             STRING,
        device_type            STRING,
        watch_duration_seconds INT,
        completion_percent     DOUBLE,
        is_late_arrival        BOOLEAN,
        event_date             DATE,
        event_time             TIMESTAMP,
        ingestion_time         TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

watch_sessions = spark.table("local.silver.watch_sessions")
dim_user       = spark.table("local.silver.dim_user").filter(col("is_current") == True)

fact = watch_sessions \
    .join(dim_user.select("user_id", "user_key"), "user_id", "left") \
    .withColumn("event_date",     to_date(col("event_time"))) \
    .withColumn("ingestion_time", current_timestamp()) \
    .select(
        "session_id", "user_key", "user_id", "content_id",
        "event_type", "device_type", "watch_duration_seconds",
        "completion_percent", "is_late_arrival",
        "event_date", "event_time", "ingestion_time"
    )

fact.writeTo("local.gold.fact_watch_sessions").overwritePartitions()
print(f"  OK: fact_watch_sessions — {fact.count()} rows")

# ════════════════════════════════════════════════════════════════════════════
# 2. daily_engagement (Dashboard table, per date)
# ════════════════════════════════════════════════════════════════════════════
print("\n--- daily_engagement ---")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.gold.daily_engagement (
        date                   DATE,
        total_users            BIGINT,
        total_sessions         BIGINT,
        total_watch_hours      DOUBLE,
        avg_watch_time_minutes DOUBLE,
        avg_completion_rate    DOUBLE,
        total_play_events      BIGINT,
        total_finish_events    BIGINT,
        finish_rate            DOUBLE,
        ingestion_time         TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

daily = fact \
    .groupBy("event_date") \
    .agg(
        countDistinct("user_id").alias("total_users"),
        count("session_id").alias("total_sessions"),
        spark_round(spark_sum("watch_duration_seconds") / 3600, 2).alias("total_watch_hours"),
        spark_round(avg("watch_duration_seconds") / 60, 2).alias("avg_watch_time_minutes"),
        spark_round(avg("completion_percent"), 2).alias("avg_completion_rate"),
        count(when(col("event_type") == "play",   True)).alias("total_play_events"),
        count(when(col("event_type") == "finish", True)).alias("total_finish_events"),
    ) \
    .withColumn("finish_rate",
        spark_round(col("total_finish_events") / col("total_sessions") * 100, 2)) \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumnRenamed("event_date", "date")

daily.writeTo("local.gold.daily_engagement").overwritePartitions()
print(f"  OK: daily_engagement — {daily.count()} rows")
daily.orderBy("date").show(5)

# ════════════════════════════════════════════════════════════════════════════
# 3. churn_features (ML feature table, per user per day)
# ════════════════════════════════════════════════════════════════════════════
print("\n--- churn_features ---")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.gold.churn_features (
        date                   DATE,
        user_id                STRING,
        age_band               STRING,
        subscription_tier      STRING,
        country                STRING,
        days_since_signup      INT,
        sessions_count         BIGINT,
        total_watch_hours      DOUBLE,
        avg_completion_rate    DOUBLE,
        finish_rate            DOUBLE,
        play_count             BIGINT,
        pause_count            BIGINT,
        stop_count             BIGINT,
        finish_count           BIGINT,
        unique_content_watched BIGINT,
        sessions_7d            BIGINT,
        watch_hours_7d         DOUBLE,
        sessions_30d           BIGINT,
        watch_hours_30d        DOUBLE,
        preferred_device       STRING,
        late_arrival_count     BIGINT,
        ingestion_time         TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

dim_user_full = spark.table("local.silver.dim_user").filter(col("is_current") == True)

churn = fact \
    .join(dim_user_full.select(
        "user_id", "age_band", "subscription_tier", "country", "signup_date"
    ), "user_id", "left") \
    .groupBy("event_date", "user_id", "age_band", "subscription_tier", "country", "signup_date") \
    .agg(
        count("session_id").alias("sessions_count"),
        spark_round(spark_sum("watch_duration_seconds") / 3600, 3).alias("total_watch_hours"),
        spark_round(avg("completion_percent"), 2).alias("avg_completion_rate"),
        count(when(col("event_type") == "play",   True)).alias("play_count"),
        count(when(col("event_type") == "pause",  True)).alias("pause_count"),
        count(when(col("event_type") == "stop",   True)).alias("stop_count"),
        count(when(col("event_type") == "finish", True)).alias("finish_count"),
        countDistinct("content_id").alias("unique_content_watched"),
        count(when(col("is_late_arrival") == True, True)).alias("late_arrival_count"),
    ) \
    .withColumn("finish_rate",
        spark_round(col("finish_count") / col("sessions_count") * 100, 2)) \
    .withColumn("days_since_signup",
        datediff(col("event_date"), to_date(col("signup_date")))) \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumnRenamed("event_date", "date")

# 7d and 30d rolling windows (self-join approximation)
churn_base = churn.alias("base")
churn_7d = fact \
    .groupBy("user_id", "event_date") \
    .agg(
        count("session_id").alias("sessions_7d"),
        spark_round(spark_sum("watch_duration_seconds") / 3600, 3).alias("watch_hours_7d")
    ).alias("w7d")

churn_30d = fact \
    .groupBy("user_id", "event_date") \
    .agg(
        count("session_id").alias("sessions_30d"),
        spark_round(spark_sum("watch_duration_seconds") / 3600, 3).alias("watch_hours_30d")
    ).alias("w30d")

preferred_device = fact \
    .groupBy("user_id", "event_date", "device_type") \
    .agg(count("session_id").alias("device_count")) \
    .groupBy(col("user_id"), col("event_date")) \
    .agg(expr("max_by(device_type, device_count)").alias("preferred_device")) \
    .alias("pd")

final_churn = churn_base \
    .join(churn_7d,
        (col("base.user_id") == col("w7d.user_id")) &
        (col("base.date") == col("w7d.event_date")), "left") \
    .join(churn_30d,
        (col("base.user_id") == col("w30d.user_id")) &
        (col("base.date") == col("w30d.event_date")), "left") \
    .join(preferred_device,
        (col("base.user_id") == col("pd.user_id")) &
        (col("base.date") == col("pd.event_date")), "left") \
    .select(
        col("base.date"),
        col("base.user_id"),
        col("base.age_band"),
        col("base.subscription_tier"),
        col("base.country"),
        col("base.days_since_signup"),
        col("base.sessions_count"),
        col("base.total_watch_hours"),
        col("base.avg_completion_rate"),
        col("base.finish_rate"),
        col("base.play_count"),
        col("base.pause_count"),
        col("base.stop_count"),
        col("base.finish_count"),
        col("base.unique_content_watched"),
        col("w7d.sessions_7d"),
        col("w7d.watch_hours_7d"),
        col("w30d.sessions_30d"),
        col("w30d.watch_hours_30d"),
        col("pd.preferred_device"),
        col("base.late_arrival_count"),
        col("base.ingestion_time")
    )

final_churn.writeTo("local.gold.churn_features").overwritePartitions()
print(f"  OK: churn_features — {final_churn.count()} rows")
final_churn.show(3)

print("\nSUCCESS: Silver -> Gold ETL completed\n")
spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, when,
    sum as spark_sum, avg, count, countDistinct,
    round as spark_round, datediff, to_date,
    max as spark_max, expr
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("GoldETL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("\n=== Silver -> Gold ETL ===\n")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")

# ── fact_watch_sessions — INCREMENTAL ────────────────────────────────────
print("--- fact_watch_sessions (incremental) ---")
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
    ) USING iceberg TBLPROPERTIES ('format-version'='2')
""")

watch_sessions = spark.table("local.silver.watch_sessions")
dim_user = spark.table("local.silver.dim_user").filter(col("is_current") == True)
existing_fact = spark.table("local.gold.fact_watch_sessions")

max_fact_ingestion = existing_fact.agg({"ingestion_time": "max"}).collect()[0][0] \
    if existing_fact.count() > 0 else None

new_sessions = watch_sessions if max_fact_ingestion is None \
    else watch_sessions.filter(col("ingestion_time") > lit(max_fact_ingestion))

new_s_count = new_sessions.count()
if new_s_count > 0:
    new_fact = new_sessions \
        .join(dim_user.select("user_id","user_key"), "user_id", "left") \
        .withColumn("event_date",     to_date(col("event_time"))) \
        .withColumn("ingestion_time", current_timestamp()) \
        .select("session_id","user_key","user_id","content_id","event_type",
                "device_type","watch_duration_seconds","completion_percent",
                "is_late_arrival","event_date","event_time","ingestion_time")
    new_fact.writeTo("local.gold.fact_watch_sessions").append()
    print(f"  OK: added {new_s_count} new fact rows")
else:
    print(f"  OK: no new sessions to process")

fact = spark.table("local.gold.fact_watch_sessions")
print(f"  fact_watch_sessions total: {fact.count()} rows")

# ── daily_engagement ─────────────────────────────────────────────────────
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
    ) USING iceberg TBLPROPERTIES ('format-version'='2')
""")

if fact.count() > 0:
    daily = fact.groupBy("event_date").agg(
        countDistinct("user_id").alias("total_users"),
        count("session_id").alias("total_sessions"),
        spark_round(spark_sum("watch_duration_seconds")/3600,2).alias("total_watch_hours"),
        spark_round(avg("watch_duration_seconds")/60,2).alias("avg_watch_time_minutes"),
        spark_round(avg("completion_percent"),2).alias("avg_completion_rate"),
        count(when(col("event_type")=="play",True)).alias("total_play_events"),
        count(when(col("event_type")=="finish",True)).alias("total_finish_events"),
    ).withColumn("finish_rate",
        spark_round(col("total_finish_events")/col("total_sessions")*100,2)) \
     .withColumn("ingestion_time", current_timestamp()) \
     .withColumnRenamed("event_date","date")
    daily.writeTo("local.gold.daily_engagement").overwritePartitions()
    print(f"  OK: daily_engagement — {daily.count()} rows")
    daily.orderBy("date").show(5)

# ── churn_features — FIXED rolling window ────────────────────────────────
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
        watch_time_30d         DOUBLE,
        avg_completion_rate    DOUBLE,
        finish_rate            DOUBLE,
        play_count             BIGINT,
        pause_count            BIGINT,
        stop_count             BIGINT,
        finish_count           BIGINT,
        unique_content_watched BIGINT,
        sessions_7d            BIGINT,
        watch_hours_7d         DOUBLE,
        favorite_genre         STRING,
        preferred_device       STRING,
        late_arrival_count     BIGINT,
        churn_label            INT,
        ingestion_time         TIMESTAMP
    ) USING iceberg TBLPROPERTIES ('format-version'='2')
""")

if fact.count() > 0:
    dim_user_full = spark.table("local.silver.dim_user").filter(col("is_current")==True)
    dim_content   = spark.table("local.silver.dim_content")

    preferred_device = fact \
        .groupBy("user_id","event_date","device_type") \
        .agg(count("session_id").alias("dc")) \
        .groupBy("user_id","event_date") \
        .agg(expr("max_by(device_type, dc)").alias("preferred_device"))

    favorite_genre = fact \
        .join(dim_content.select("content_id","genre"), "content_id", "left") \
        .groupBy("user_id","event_date","genre") \
        .agg(count("session_id").alias("gc")) \
        .groupBy("user_id","event_date") \
        .agg(expr("max_by(genre, gc)").alias("favorite_genre"))

    daily_user = fact \
        .join(dim_user_full.select("user_id","age_band","subscription_tier","country","signup_date"), "user_id", "left") \
        .join(preferred_device, ["user_id","event_date"], "left") \
        .join(favorite_genre,   ["user_id","event_date"], "left") \
        .groupBy("event_date","user_id","age_band","subscription_tier","country","signup_date","preferred_device","favorite_genre") \
        .agg(
            count("session_id").alias("sessions_count"),
            spark_round(spark_sum("watch_duration_seconds")/3600,3).alias("total_watch_hours"),
            spark_round(avg("completion_percent"),2).alias("avg_completion_rate"),
            count(when(col("event_type")=="play",True)).alias("play_count"),
            count(when(col("event_type")=="pause",True)).alias("pause_count"),
            count(when(col("event_type")=="stop",True)).alias("stop_count"),
            count(when(col("event_type")=="finish",True)).alias("finish_count"),
            countDistinct("content_id").alias("unique_content_watched"),
            count(when(col("is_late_arrival")==True,True)).alias("late_arrival_count"),
        ) \
        .withColumn("finish_rate",
            spark_round(col("finish_count")/col("sessions_count")*100,2)) \
        .withColumn("days_since_signup",
            datediff(col("event_date"), to_date(col("signup_date")))) \
        .withColumnRenamed("event_date","date")

    # FIX: date.cast("long") = epoch DAYS, so rangeBetween uses DAYS not seconds
    w7  = Window.partitionBy("user_id") \
        .orderBy(col("date").cast("long")) \
        .rangeBetween(-7, 0)   # 7 days
    w30 = Window.partitionBy("user_id") \
        .orderBy(col("date").cast("long")) \
        .rangeBetween(-30, 0)  # 30 days

    churn = daily_user \
        .withColumn("sessions_7d",
            spark_sum("sessions_count").over(w7)) \
        .withColumn("watch_hours_7d",
            spark_round(spark_sum("total_watch_hours").over(w7), 3)) \
        .withColumn("watch_time_30d",
            spark_round(spark_sum("total_watch_hours").over(w30), 3)) \
        .withColumn("churn_label",
            when(col("finish_rate") < 10, 1)
            .when((col("sessions_count") <= 1) & (col("days_since_signup") > 30), 1)
            .otherwise(0)) \
        .withColumn("ingestion_time", current_timestamp()) \
        .select("date","user_id","age_band","subscription_tier","country",
                "days_since_signup","sessions_count","total_watch_hours",
                "watch_time_30d","avg_completion_rate","finish_rate",
                "play_count","pause_count","stop_count","finish_count",
                "unique_content_watched","sessions_7d","watch_hours_7d",
                "favorite_genre","preferred_device","late_arrival_count",
                "churn_label","ingestion_time")

    churn.writeTo("local.gold.churn_features").overwritePartitions()
    print(f"  OK: churn_features — {churn.count()} rows")
    churn.show(3)

print("\nSUCCESS: Silver -> Gold ETL completed\n")
spark.stop()

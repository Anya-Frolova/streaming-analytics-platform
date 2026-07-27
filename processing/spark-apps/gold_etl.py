from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when, sum as spark_sum, avg, count, countDistinct, round as spark_round, datediff, to_date, max as spark_max, expr

spark = SparkSession.builder.appName("GoldETL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("\n=== Silver -> Gold ETL ===\n")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")

# fact_watch_sessions
print("--- fact_watch_sessions ---")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.gold.fact_watch_sessions (
        session_id STRING, user_key BIGINT, user_id STRING,
        content_id STRING, event_type STRING, device_type STRING,
        watch_duration_seconds INT, completion_percent DOUBLE,
        is_late_arrival BOOLEAN, event_date DATE,
        event_time TIMESTAMP, ingestion_time TIMESTAMP
    ) USING iceberg TBLPROPERTIES ('format-version'='2')
""")

watch_sessions = spark.table("local.silver.watch_sessions")
dim_user = spark.table("local.silver.dim_user").filter(col("is_current")==True)

if spark.table("local.gold.fact_watch_sessions").count() == 0 and watch_sessions.count() > 0:
    fact = watch_sessions \
        .join(dim_user.select("user_id","user_key"), "user_id", "left") \
        .withColumn("event_date",     to_date(col("event_time"))) \
        .withColumn("ingestion_time", current_timestamp()) \
        .select("session_id","user_key","user_id","content_id","event_type",
                "device_type","watch_duration_seconds","completion_percent",
                "is_late_arrival","event_date","event_time","ingestion_time")
    fact.writeTo("local.gold.fact_watch_sessions").append()
    print(f"  OK: fact_watch_sessions — {fact.count()} rows")
else:
    fact = spark.table("local.gold.fact_watch_sessions")
    print(f"  OK: fact_watch_sessions — {fact.count()} rows")

fact = spark.table("local.gold.fact_watch_sessions")

# daily_engagement
print("\n--- daily_engagement ---")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.gold.daily_engagement (
        date DATE, total_users BIGINT, total_sessions BIGINT,
        total_watch_hours DOUBLE, avg_watch_time_minutes DOUBLE,
        avg_completion_rate DOUBLE, total_play_events BIGINT,
        total_finish_events BIGINT, finish_rate DOUBLE, ingestion_time TIMESTAMP
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
else:
    print("  INFO: no data yet")

# churn_features
print("\n--- churn_features ---")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.gold.churn_features (
        date DATE, user_id STRING, age_band STRING,
        subscription_tier STRING, country STRING,
        days_since_signup INT, sessions_count BIGINT,
        total_watch_hours DOUBLE, avg_completion_rate DOUBLE,
        finish_rate DOUBLE, play_count BIGINT, pause_count BIGINT,
        stop_count BIGINT, finish_count BIGINT,
        unique_content_watched BIGINT, sessions_7d BIGINT,
        watch_hours_7d DOUBLE, preferred_device STRING,
        late_arrival_count BIGINT, ingestion_time TIMESTAMP
    ) USING iceberg TBLPROPERTIES ('format-version'='2')
""")

if fact.count() > 0:
    dim_user_full = spark.table("local.silver.dim_user").filter(col("is_current")==True)
    preferred_device = fact \
        .groupBy("user_id","event_date","device_type") \
        .agg(count("session_id").alias("dc")) \
        .groupBy("user_id","event_date") \
        .agg(expr("max_by(device_type, dc)").alias("preferred_device"))

    churn = fact \
        .join(dim_user_full.select("user_id","age_band","subscription_tier","country","signup_date"), "user_id", "left") \
        .join(preferred_device, ["user_id","event_date"], "left") \
        .groupBy("event_date","user_id","age_band","subscription_tier","country","signup_date","preferred_device") \
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
            count("session_id").alias("sessions_7d"),
            spark_round(spark_sum("watch_duration_seconds")/3600,3).alias("watch_hours_7d"),
        ) \
        .withColumn("finish_rate", spark_round(col("finish_count")/col("sessions_count")*100,2)) \
        .withColumn("days_since_signup", datediff(col("event_date"), to_date(col("signup_date")))) \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumnRenamed("event_date","date") \
        .select("date","user_id","age_band","subscription_tier","country",
                "days_since_signup","sessions_count","total_watch_hours",
                "avg_completion_rate","finish_rate","play_count","pause_count",
                "stop_count","finish_count","unique_content_watched",
                "sessions_7d","watch_hours_7d","preferred_device",
                "late_arrival_count","ingestion_time")
    churn.writeTo("local.gold.churn_features").overwritePartitions()
    print(f"  OK: churn_features — {churn.count()} rows")
    churn.show(3)
else:
    print("  INFO: no data yet")

print("\nSUCCESS: Silver -> Gold ETL completed\n")
spark.stop()

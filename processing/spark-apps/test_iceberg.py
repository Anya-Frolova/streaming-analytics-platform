"""
Connectivity test: Spark -> Iceberg REST -> MinIO
Run: docker exec spark-master spark-submit /opt/spark-apps/test_iceberg.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IcebergConnectivityTest").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== Testing Iceberg + MinIO connectivity ===\n")

for ns in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{ns}")
    print(f"  OK: namespace local.{ns} ready")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.bronze.connectivity_test (
        id       BIGINT,
        message  STRING,
        ts       TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

spark.sql("""
    INSERT INTO local.bronze.connectivity_test VALUES
    (1, 'Spark + Iceberg works!', current_timestamp()),
    (2, 'MinIO is working',       current_timestamp())
""")

df = spark.table("local.bronze.connectivity_test")
df.show(truncate=False)
print(f"\nSUCCESS: {df.count()} rows in MinIO via Iceberg\n")
spark.stop()

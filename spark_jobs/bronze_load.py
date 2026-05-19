from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, input_file_name, current_timestamp, to_date
import os

# =========================
# CONFIG
# =========================

RAW_PATH = "data/raw/events"
BRONZE_PATH = "data/bronze/events"

os.makedirs(BRONZE_PATH, exist_ok=True)

# =========================
# SPARK SESSION
# =========================

spark = SparkSession.builder \
    .appName("bronze_load") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# =========================
# LOAD RAW JSON
# =========================

df = spark.read.json(RAW_PATH)


# =========================
# ENRICH BRONZE DATA
# =========================

df_bronze = (
    df
    .withColumn("ingested_processing_ts", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("event_date", to_date(col("event_ts")))
)


# =========================
# WRITE BRONZE PARQUET
# =========================

(
    df_bronze
    .write
    .mode("append")
    .partitionBy("event_date", "event_type")
    .parquet(BRONZE_PATH)
)


print("Bronze layer build complete:")
print(f"Input:  {RAW_PATH}")
print(f"Output: {BRONZE_PATH}")
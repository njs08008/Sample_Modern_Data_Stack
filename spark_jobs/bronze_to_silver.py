from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, lit, current_date, date_sub
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import os
import json
from datetime import datetime

# =========================
# CONFIG
# =========================

BRONZE_PATH = "data/bronze/events"
SILVER_PATH = "data/silver/events"

STATE_PATH = "data/_state/silver_checkpoint.json"

LOOKBACK_DAYS = 2  # safety net for late data

os.makedirs(SILVER_PATH, exist_ok=True)
os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)

# =========================
# STATE HELPERS
# =========================

def load_state():
    if not os.path.exists(STATE_PATH):
        return {"last_processed_event_date": "1970-01-01"}

    with open(STATE_PATH, "r") as f:
        return json.load(f)


def save_state(state):
    with open(STATE_PATH, "w") as f:
        json.dump(state, f)


# =========================
# SPARK
# =========================

spark = (
    SparkSession.builder
    .appName("SilverLayerIncrementalWithCheckpoint")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================
# LOAD STATE
# =========================

state = load_state()
last_processed_date = state["last_processed_event_date"]

print(f"Last processed event_date: {last_processed_date}")


# =========================
# LOAD BRONZE (partition pruned)
# =========================

df = (
    spark.read.parquet(BRONZE_PATH)
    .where(
        col("event_date") >= date_sub(to_date(lit(last_processed_date)), LOOKBACK_DAYS)
    )
)


# =========================
# NORMALIZE
# =========================

df = df.withColumn("event_ts", to_timestamp(col("event_ts")))


# =========================
# DEDUPLICATION
# =========================

window = Window.partitionBy("event_id").orderBy(col("ingested_processing_ts").desc())

df = (
    df.withColumn("rn", row_number().over(window))
      .filter(col("rn") == 1)
      .drop("rn")
)


# =========================
# SILVER TRANSFORM
# =========================

df_silver = df.select(
    col("event_id"),
    col("event_type"),
    col("event_ts"),
    col("event_date"),
    col("user_id"),
    col("country"),
    col("page"),
    col("query"),
    col("amount").cast("double"),
    col("error_message"),
    col("ingested_at"),
    col("ingested_processing_ts"),
    col("source_file")
)


# =========================
# WRITE SILVER
# =========================

(
    df_silver
    .write
    .mode("overwrite")
    .partitionBy("event_date", "event_type")
    .parquet(SILVER_PATH)
)


# =========================
# UPDATE STATE
# =========================

max_date = df.agg({"event_date": "max"}).collect()[0][0]

if max_date:
    new_state = {
        "last_processed_event_date": str(max_date)
    }
    save_state(new_state)

    print(f"Updated checkpoint → {new_state}")
else:
    print("No new data processed; state unchanged")


print("Silver incremental run complete")
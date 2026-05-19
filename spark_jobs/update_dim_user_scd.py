import os
import duckdb
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# =====================================================
# CONFIG
# =====================================================

SILVER_PATH = "data/silver/events"
DUCKDB_PATH = "data/warehouse.duckdb"

STATE_DIR = "state"
WATERMARK_FILE = f"{STATE_DIR}/scd_watermark.txt"

DEFAULT_WATERMARK = "1970-01-01 00:00:00"

# =====================================================
# SPARK SESSION
# =====================================================

spark = SparkSession.builder \
    .appName("update_dim_user_scd") \
    .getOrCreate()

# =====================================================
# LOAD WATERMARK
# =====================================================

os.makedirs(STATE_DIR, exist_ok=True)

if os.path.exists(WATERMARK_FILE):
    with open(WATERMARK_FILE, "r") as f:
        last_watermark = f.read().strip()
else:
    last_watermark = DEFAULT_WATERMARK

print(f"Last watermark: {last_watermark}")

# =====================================================
# LOAD SILVER EVENTS
# =====================================================

df = spark.read.parquet(SILVER_PATH)

df = df.withColumn(
    "ingested_at",
    to_timestamp(col("ingested_at"))
)

# =====================================================
# FILTER TO NEW EVENTS ONLY
# =====================================================

df_incremental = df.filter(
    col("ingested_at") > last_watermark
)

# =====================================================
# SELECT DIMENSION FIELDS
# =====================================================

df_incremental = df_incremental.select(
    "user_id",
    "country",
    "event_ts",
    "ingested_at"
).dropna(subset=["user_id", "country"])

# =====================================================
# SHORT CIRCUIT IF NO NEW DATA
# =====================================================

if df_incremental.rdd.isEmpty():
    print("No new events to process")
    spark.stop()
    exit()

# =====================================================
# CONVERT TO PANDAS
# =====================================================

events = df_incremental.toPandas()

# =====================================================
# CONNECT TO DUCKDB
# =====================================================

conn = duckdb.connect(DUCKDB_PATH)

# =====================================================
# CREATE TABLE IF NOT EXISTS
# =====================================================

conn.execute("""
CREATE TABLE IF NOT EXISTS dim_user_scd (
    user_id BIGINT,
    country VARCHAR,
    effective_start TIMESTAMP,
    effective_end TIMESTAMP,
    is_current BOOLEAN
)
""")

# =====================================================
# SORT EVENTS
# IMPORTANT FOR SCD CORRECTNESS
# LATE EVENTS CAN STILL MAKE THIS GO WRONG
# =====================================================

events = events.sort_values("event_ts")

# =====================================================
# PROCESS EVENTS
# =====================================================

for _, row in events.iterrows():

    user_id = int(row["user_id"])
    country = row["country"]
    event_ts = row["event_ts"]

    current = conn.execute(f"""
        SELECT country
        FROM dim_user_scd
        WHERE user_id = {user_id}
          AND is_current = TRUE
    """).fetchone()

    # -------------------------------------------------
    # FIRST USER RECORD
    # -------------------------------------------------

    if current is None:

        conn.execute(f"""
            INSERT INTO dim_user_scd
            VALUES (
                {user_id},
                '{country}',
                '{event_ts}',
                NULL,
                TRUE
            )
        """)

        continue

    current_country = current[0]

    # -------------------------------------------------
    # NO CHANGE
    # -------------------------------------------------

    if current_country == country:
        continue

    # -------------------------------------------------
    # COUNTRY CHANGE
    # -------------------------------------------------

    conn.execute(f"""
        UPDATE dim_user_scd
        SET
            effective_end = '{event_ts}',
            is_current = FALSE
        WHERE user_id = {user_id}
          AND is_current = TRUE
    """)

    conn.execute(f"""
        INSERT INTO dim_user_scd
        VALUES (
            {user_id},
            '{country}',
            '{event_ts}',
            NULL,
            TRUE
        )
    """)

# =====================================================
# UPDATE WATERMARK
# =====================================================

max_watermark = events["ingested_at"].max()

with open(WATERMARK_FILE, "w") as f:
    f.write(str(max_watermark))

print(f"Updated watermark: {max_watermark}")

print("Incremental SCD Type 2 update complete")

spark.stop()
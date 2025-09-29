# 02_dlt_pipeline.py
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

LANDING = spark.conf.get("landing_path", "/Volumes/crypto/core/landing/markets")

# -------------------------
# BRONZE: raw arrivals
# -------------------------
@dlt.table(
    name="prices_bronze",
    comment="Raw CoinGecko JSON from landing volume"
)
def prices_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.rescuedDataColumn", "_rescued")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(LANDING)
        .withColumn("load_ts", current_timestamp())
    )

# -------------------------
# SILVER: typed, parsed, idempotent (streaming-safe dedupe)
# -------------------------
@dlt.expect("valid_price", "price > 0")
@dlt.expect_or_drop(
    "has_core_fields",
    "asset IS NOT NULL AND symbol IS NOT NULL AND name IS NOT NULL AND event_ts IS NOT NULL"
)
@dlt.table(
    name="prices_silver",
    comment="Typed, parsed, and deduplicated quotes (streaming-safe)"
)
def prices_silver():
    fmt_ms = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    fmt_s  = "yyyy-MM-dd'T'HH:mm:ss'Z'"

    s = (
        dlt.read_stream("prices_bronze")
        .withColumn("asset", lower(col("id")))
        .withColumn("symbol", lower(col("symbol")))
        .withColumn("price", col("current_price").cast("double"))
        .withColumn("volume", col("total_volume").cast("double"))
        .withColumn("event_ts", coalesce(to_timestamp("last_updated", fmt_ms),
                                         to_timestamp("last_updated", fmt_s)))
        .withColumn("ingest_ts", coalesce(to_timestamp("ingest_ts", fmt_ms),
                                          to_timestamp("ingest_ts", fmt_s)))
        .withColumn("event_key",
            concat_ws("|", col("asset"),
                      date_format(col("event_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'")))
        .withWatermark("event_ts", "20 minutes")
        .dropDuplicates(["event_key"])  # streaming-safe idempotency
    )

    return s.select("asset","symbol","name","price","volume","event_ts","ingest_ts","load_ts","event_key")

# -------------------------
# GOLD: metrics for anomaly detection
# -------------------------
@dlt.table(
    name="metrics_gold",
    comment="5-minute windowed metrics per asset (avg/vol/min/max/volume, returns)"
)
def metrics_gold():
    s = dlt.read_stream("prices_silver").withWatermark("event_ts", "2 minutes")
    agg = (
        s.groupBy(window(col("event_ts"), "5 minutes"), col("asset"), col("symbol"))
         .agg(
            avg("price").alias("price_avg"),
            stddev("price").alias("price_vol"),
            min("price").alias("price_min"),
            max("price").alias("price_max"),
            sum("volume").alias("vol_sum")
         )
         .withColumn("window_start", col("window").start)
         .withColumn("window_end", col("window").end)
         .drop("window")
    )

    return agg.withColumn("ret_window", (col("price_max")/col("price_min") - 1.0))

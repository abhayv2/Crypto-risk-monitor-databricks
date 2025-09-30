# 04_infer_iforest.py
# Batch scoring for anomaly detection. Compute ret_prev, load UC model, MERGE results.

import numpy as np
import pandas as pd
import mlflow
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ----------------------------
# Config
# ----------------------------
UC_MODEL_NAME   = "crypto.core.crypto_anomaly_iforest"  # UC Models path
USE_ALIAS       = "champion"                            # set to None to use Latest
LOOKBACK_DAYS   = 7                                     # compute features over last N days (for ret_prev continuity)
MAX_ROWS        = 200_000                               # safety cap for Free Edition

FEATURES = ["price_avg", "price_vol", "ret_window", "vol_sum", "ret_prev"]

# ----------------------------
# Tables (create if needed)
# ----------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS crypto.core.anomalies (
  asset STRING,
  symbol STRING,
  window_end TIMESTAMP,
  price_avg DOUBLE,
  price_vol DOUBLE,
  ret_window DOUBLE,
  vol_sum DOUBLE,
  ret_prev DOUBLE,
  anomaly_score DOUBLE,
  is_anomaly INT,
  model_version STRING,
  scored_at TIMESTAMP
) USING DELTA
""")

# ----------------------------
# Resume watermark from target
# ----------------------------
row = spark.sql("SELECT max(window_end) AS m FROM crypto.core.anomalies").collect()[0]
existing_max = row["m"]  # None if table empty

# ----------------------------
# Prepare Gold with ret_prev (batch safe)
# ----------------------------
gold = (
    spark.table("crypto.core.metrics_gold")
         .where(F.col("window_end") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
         .select("asset","symbol","window_end","price_avg","price_vol","ret_window","vol_sum")
)

w = Window.partitionBy("asset").orderBy("window_end")
gold = gold.withColumn("ret_prev", (F.col("price_avg")/F.lag("price_avg", 1).over(w) - 1.0))

# Score only new windows (strictly greater than existing_max)
if existing_max is not None:
    gold = gold.where(F.col("window_end") > F.lit(existing_max))

# Keep size manageable
gold = gold.orderBy(F.col("window_end").asc()).limit(MAX_ROWS)

count_to_score = gold.count()
if count_to_score == 0:
    print("Nothing new to score.")
    dbutils.notebook.exit("OK")  # makes scheduled jobs fast-exit
else:
    print(f"Scoring {count_to_score} rows.")

pdf = gold.toPandas()

# ----------------------------
# Feature cleanup (null/inf -> 0.0)
# ----------------------------
pdf[FEATURES] = (
    pdf[FEATURES]
      .replace([np.inf, -np.inf], np.nan)
      .fillna(0.0)
      .astype(float)
)

# ----------------------------
# Load model from UC Models
# ----------------------------
mlflow.set_registry_uri("databricks-UC")

from mlflow.tracking import MlflowClient
client = MlflowClient()

model_version = None
if USE_ALIAS:
    # Try alias first
    try:
        mv = client.get_model_version_by_alias(UC_MODEL_NAME, USE_ALIAS)
        model_version = mv.version
        model_uri = f"models:/{UC_MODEL_NAME}@{USE_ALIAS}"
    except Exception as e:
        print(f"Alias '{USE_ALIAS}' not found; falling back to Latest. Details: {e}")
        model_uri = f"models:/{UC_MODEL_NAME}/Latest"
else:
    model_uri = f"models:/{UC_MODEL_NAME}/Latest"

model = mlflow.sklearn.load_model(model_uri)

# If version unknown (e.g., using Latest), try to fetch the highest version
if model_version is None:
    try:
        # UC supports aliases; Latest isn't an alias. We best-effort the max version:
        mvs = client.search_model_versions(f"name='{UC_MODEL_NAME}'")
        if mvs:
            model_version = str(max(int(m.version) for m in mvs))
    except Exception:
        model_version = None

# ----------------------------
# Score
# ----------------------------
X = pdf[FEATURES].astype(float)
scores = model.decision_function(X)         # higher = more normal
labels = model.predict(X)                   # -1 anomaly, 1 normal
is_anom = (labels == -1).astype(int)

pdf_out = pd.DataFrame({
    "asset":        pdf["asset"].values,
    "symbol":       pdf["symbol"].values,
    "window_end":   pdf["window_end"].values,
    "price_avg":    pdf["price_avg"].values,
    "price_vol":    pdf["price_vol"].values,
    "ret_window":   pdf["ret_window"].values,
    "vol_sum":      pdf["vol_sum"].values,
    "ret_prev":     pdf["ret_prev"].values,
    "anomaly_score": scores,
    "is_anomaly":    is_anom,
    "model_version": model_version if model_version is not None else "",
    "scored_at":     pd.Timestamp.utcnow()
})

# ----------------------------
# MERGE (idempotent upsert)
# ----------------------------
scored_sdf = spark.createDataFrame(pdf_out)
scored_sdf.createOrReplaceTempView("scored_batch")

spark.sql("""
MERGE INTO crypto.core.anomalies t
USING scored_batch s
ON  t.asset = s.asset AND t.window_end = s.window_end
WHEN MATCHED THEN UPDATE SET
  t.symbol        = s.symbol,
  t.price_avg     = s.price_avg,
  t.price_vol     = s.price_vol,
  t.ret_window    = s.ret_window,
  t.vol_sum       = s.vol_sum,
  t.ret_prev      = s.ret_prev,
  t.anomaly_score = s.anomaly_score,
  t.is_anomaly    = s.is_anomaly,
  t.model_version = s.model_version,
  t.scored_at     = s.scored_at
WHEN NOT MATCHED THEN INSERT *
""")

print(f"Upserted {scored_sdf.count()} scored rows into crypto.core.anomalies.")

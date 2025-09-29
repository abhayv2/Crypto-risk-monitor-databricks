# 03_train_iforest.py
# Train an IsolationForest on Gold features and register the model in Unity Catalog.

import numpy as np
import pandas as pd

import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature

from sklearn.ensemble import IsolationForest
from sklearn.model_selection import train_test_split

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ----------------------------
# Configs (tweak as needed)
# ----------------------------
UC_MODEL_NAME   = "crypto.core.crypto_anomaly_iforest"   # <catalog>.<schema>.<name>
EXPERIMENT_PATH = "/Shared/crypto-risk/iforest-experiments"  # optional; appears in MLflow UI

LOOKBACK_DAYS = 7       # train on last N days
MAX_ROWS      = 200_000 # cap for Free Edition
N_ESTIMATORS  = 200
CONTAMINATION = 0.02
RANDOM_STATE  = 42

# ----------------------------
# MLflow setup
# ----------------------------
try:
    mlflow.set_experiment(EXPERIMENT_PATH)
except Exception:
    pass  # it's OK if it doesn't exist; Databricks will create it if you have perms

# IMPORTANT: use Unity Catalog model registry (workspace registry is disabled)
mlflow.set_registry_uri("databricks-UC")

# ----------------------------
# Load training data from Gold
# ----------------------------
gold = (
    spark.table("crypto.core.metrics_gold")
         .where(F.col("window_end") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
         .select("asset","symbol","window_end","price_avg","price_vol","ret_window","vol_sum")
)

count_gold = gold.count()
if count_gold == 0:
    raise ValueError(
        "No rows in crypto.core.metrics_gold for the selected lookback window. "
        "Let the DLT pipeline run a bit longer or reduce LOOKBACK_DAYS."
    )

# Compute previous-window % change in Spark (batch-safe; not allowed in streaming)
w = Window.partitionBy("asset").orderBy("window_end")
gold = gold.withColumn("ret_prev", (F.col("price_avg")/F.lag("price_avg", 1).over(w) - 1.0))

# Keep dataset size manageable AFTER adding ret_prev
gold = gold.orderBy(F.col("window_end").desc()).limit(MAX_ROWS)

# Convert to pandas once
pdf = gold.toPandas()

# ----------------------------
# Feature engineering (batch)
# ----------------------------
FEATURES = ["price_avg", "price_vol", "ret_window", "vol_sum", "ret_prev"]

# Handle NaNs/inf (sample stddev can be null; first ret_prev is null)
pdf[FEATURES] = (
    pdf[FEATURES]
      .replace([np.inf, -np.inf], np.nan)
      .fillna(0.0)
      .astype(float)
)

# Optional quick holdout just to track unsupervised score stats
if len(pdf) > 1000:
    train_df, holdout_df = train_test_split(pdf, test_size=0.2, shuffle=True, random_state=RANDOM_STATE)
else:
    train_df, holdout_df = pdf, None

X_train = train_df[FEATURES]
X_hold  = holdout_df[FEATURES] if holdout_df is not None else None

# ----------------------------
# Train & track with MLflow
# ----------------------------
with mlflow.start_run(run_name="iforest_train"):
    model = IsolationForest(
        n_estimators=N_ESTIMATORS,
        contamination=CONTAMINATION,
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(X_train)

    # Log params/metrics
    mlflow.log_params({
        "lookback_days": LOOKBACK_DAYS,
        "max_rows": int(MAX_ROWS),
        "n_estimators": N_ESTIMATORS,
        "contamination": CONTAMINATION,
        "features": ",".join(FEATURES),
        "random_state": RANDOM_STATE,
        "rows_train": int(len(X_train)),
        "rows_total": int(len(pdf)),
    })

    train_scores = model.decision_function(X_train.to_numpy())
    mlflow.log_metric("train_score_mean", float(np.mean(train_scores)))
    mlflow.log_metric("train_score_std",  float(np.std(train_scores)))

    if X_hold is not None and len(X_hold) > 0:
        hold_scores = model.decision_function(X_hold.to_numpy())
        mlflow.log_metric("hold_rows", int(len(X_hold)))
        mlflow.log_metric("hold_score_mean", float(np.mean(hold_scores)))
        mlflow.log_metric("hold_score_std",  float(np.std(hold_scores)))

    # Log model with signature for serving/reuse
    input_example = X_train.iloc[[0]].copy()
    signature = infer_signature(X_train, model.decision_function(X_train.iloc[:5].to_numpy()))
    mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        signature=signature,
        input_example=input_example
    )

    run_id = mlflow.active_run().info.run_id
    model_uri = f"runs:/{run_id}/model"
    print("Logged model at:", model_uri)

    # ----------------------------
    # Register to Unity Catalog Models
    # ----------------------------
    try:
        registered = mlflow.register_model(model_uri=model_uri, name=UC_MODEL_NAME)
        print(f"Registered UC model: {registered.name} v{registered.version}")

        # Optional: set/advance alias for stable inference target
        try:
            from mlflow.tracking import MlflowClient
            MlflowClient().set_registered_model_alias(UC_MODEL_NAME, "champion", int(registered.version))
            print("Alias 'champion' set on UC model.")
        except Exception as e:
            print("Skipped alias set:", repr(e))

    except Exception as e:
        print("UC registration error:", repr(e))

print("Training complete. Check MLflow Experiments and UC Models for details.")

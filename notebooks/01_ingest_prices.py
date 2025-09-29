import requests, pandas as pd
from datetime import datetime, timezone
from pyspark.sql import functions as F

SYMS = ["bitcoin","ethereum","solana","cardano","dogecoin"]
URL = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "ids": ",".join(SYMS),
    "order": "market_cap_desc",
    "per_page": 50,
    "page": 1,
    "sparkline": "false",
    "price_change_percentage": "1h,24h,7d",
}

# UTC timestamp
ts = datetime.now(timezone.utc).replace(microsecond=0)
date_str = ts.strftime("%Y-%m-%d")
hour_str = ts.strftime("%H")
ingest_ts = ts.strftime("%Y-%m-%dT%H:%M:%SZ")   

landing_dir = "/Volumes/crypto/core/landing/markets"

r = requests.get(URL, params=params, headers={"User-Agent": "dbx-free-ingest"}, timeout=15)
r.raise_for_status()
data = r.json()
for d in data:
    d["ingest_ts"] = ingest_ts

pdf = pd.json_normalize(data)
sdf = spark.createDataFrame(pdf)

# Partition by date/hour so Auto Loader can pick up new folders
(sdf.withColumn("date", F.lit(date_str))
    .withColumn("hour", F.lit(hour_str))
    .coalesce(1)
    .write.mode("append")
    .partitionBy("date", "hour")
    .json(landing_dir))

print(f"Wrote {len(data)} records under {landing_dir}/date={date_str}/hour={hour_str}")
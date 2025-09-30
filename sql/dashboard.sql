CREATE OR REPLACE VIEW crypto.core.vw_anomalies_recent AS
SELECT *
FROM crypto.core.anomalies
WHERE window_end >= current_timestamp() - INTERVAL 7 DAYS;

CREATE OR REPLACE VIEW crypto.core.vw_price_with_flags AS
SELECT g.window_end, g.asset, g.symbol, g.price_avg,
       COALESCE(a.is_anomaly, 0) AS is_anomaly
FROM crypto.core.metrics_gold g
LEFT JOIN crypto.core.anomalies a
  ON a.asset = g.asset AND a.window_end = g.window_end; 



-- Recent anomalies
SELECT asset, symbol, window_end, price_avg, price_vol, ret_window, vol_sum, anomaly_score, is_anomaly, scored_at
FROM crypto.core.anomalies
WHERE window_end > current_timestamp() - INTERVAL 1 DAY
ORDER BY window_end DESC;

-- Price and anomaly markers
SELECT g.window_end, g.asset, g.symbol, g.price_avg,
       COALESCE(a.is_anomaly, 0) AS is_anomaly
FROM crypto.core.metrics_gold g
LEFT JOIN crypto.core.anomalies a
  ON g.asset = a.asset AND g.symbol = a.symbol AND g.window_end = a.window_end
WHERE g.window_end > current_timestamp() - INTERVAL 1 DAY
ORDER BY g.window_end DESC;

-- Anomaly leaderboard (24h)
SELECT asset,
       COUNT_IF(is_anomaly = 1) AS anomalies_24h,
       AVG(anomaly_score)       AS avg_score,
       MAX(window_end)          AS last_event
FROM crypto.core.anomalies
WHERE window_end >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY asset
ORDER BY anomalies_24h DESC, avg_score ASC;

-- Anomaly rate vs. total windows (last 24h)
WITH totals AS (
  SELECT COUNT(*) AS total_windows
  FROM crypto.core.metrics_gold
  WHERE window_end >= current_timestamp() - INTERVAL 24 HOURS
),
anoms AS (
  SELECT COUNT(*) AS anomalies
  FROM crypto.core.anomalies
  WHERE window_end >= current_timestamp() - INTERVAL 24 HOURS
    AND is_anomaly = 1
)
SELECT a.anomalies, t.total_windows,
       a.anomalies / t.total_windows AS anomaly_rate
FROM anoms a CROSS JOIN totals t;


-- Score distribution (pick an asset)
SELECT CAST(FLOOR(anomaly_score * 100) / 100 AS DOUBLE) AS score_bin,
       COUNT(*) AS cnt
FROM crypto.core.anomalies
WHERE asset = 'bitcoin'
  AND window_end >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY score_bin
ORDER BY score_bin;


--Price line + anomaly markers (per asset):
WITH series AS (
  SELECT g.window_end, g.price_avg,
         COALESCE(a.is_anomaly, 0) AS is_anomaly
  FROM crypto.core.metrics_gold g
  LEFT JOIN crypto.core.anomalies a
    ON a.asset = g.asset AND a.window_end = g.window_end
  WHERE g.asset = :asset
    AND g.window_end BETWEEN :from_ts AND :to_ts
)
SELECT window_end, price_avg,
       CASE WHEN is_anomaly = 1 THEN price_avg END AS anomaly_price
FROM series
ORDER BY window_end;
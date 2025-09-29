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
CREATE DATABASE IF NOT EXISTS poc_stream;

CREATE TABLE IF NOT EXISTS poc_stream.crypto_trades (
  symbol         VARCHAR(20)    NOT NULL,
  trade_id       BIGINT         NOT NULL,
  price          DECIMAL(18, 8) NULL,
  quantity       DECIMAL(18, 8) NULL,
  quote_volume   DECIMAL(24, 8) NULL,
  trade_time     DATETIME       NULL,
  is_buyer_maker BOOLEAN        NULL
)
ENGINE = OLAP
PRIMARY KEY (symbol, trade_id)
DISTRIBUTED BY HASH(symbol) BUCKETS 4
ORDER BY (symbol, trade_time)
PROPERTIES (
  "compression" = "LZ4",
  "enable_persistent_index" = "true",
  "replication_num" = "1"
);

CREATE MATERIALIZED VIEW IF NOT EXISTS poc_stream.mv_market_1min
(symbol, candle_minute, trades_count, low_price, high_price, total_base_volume, total_usdt_volume)
DISTRIBUTED BY RANDOM
REFRESH SCHEDULE EVERY(INTERVAL 60 SECOND)
PROPERTIES ("replication_num" = "1")
AS SELECT
    symbol,
    date_trunc('minute', trade_time) AS candle_minute,
    COUNT(*)            AS trades_count,
    MIN(price)          AS low_price,
    MAX(price)          AS high_price,
    SUM(quantity)       AS total_base_volume,
    SUM(quote_volume)   AS total_usdt_volume
FROM poc_stream.crypto_trades
GROUP BY symbol, date_trunc('minute', trade_time);

SELECT COUNT(*) AS rows_total,
       MAX(trade_time) AS last_trade_utc,
       TIMESTAMPDIFF(SECOND, MAX(trade_time), UTC_TIMESTAMP()) AS lag_seconds
FROM poc_stream.crypto_trades;

SELECT COUNT(*) AS rows_checked,
       SUM(CASE WHEN ABS(quote_volume - price * quantity) > 0.000000006 THEN 1 ELSE 0 END) AS bad_rows
FROM poc_stream.crypto_trades;

SELECT * FROM poc_stream.mv_market_1min
ORDER BY candle_minute DESC, symbol LIMIT 10;

SELECT task_name, state, create_time, finish_time
FROM information_schema.task_runs
WHERE `database` = 'poc_stream'
ORDER BY create_time DESC
LIMIT 10;

SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';
SET 'execution.checkpointing.interval' = '10 s';

CREATE TABLE flink_binance_source (
    `stream` STRING,
    `data` ROW<`s` STRING, `t` BIGINT, `p` DECIMAL(18,8),
               `q` DECIMAL(18,8), `T` BIGINT, `m` BOOLEAN>
) WITH (
    'connector' = 'filesystem',
    'path' = '/data/binance',
    'format' = 'json',
    'source.monitor-interval' = '2 s'
);

CREATE TABLE sr_crypto_trades (
    symbol STRING,
    trade_id BIGINT,
    price DECIMAL(18, 8),
    quantity DECIMAL(18, 8),
    quote_volume DECIMAL(24, 8),
    trade_time TIMESTAMP(3),
    is_buyer_maker BOOLEAN,
    PRIMARY KEY (symbol, trade_id) NOT ENFORCED
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://quickstart:9030',   -- <STARROCKS_HOST>:9030
    'load-url' = 'quickstart:8030',                -- <STARROCKS_HOST>:8030
    'database-name' = 'poc_stream',
    'table-name' = 'crypto_trades',
    'username' = 'root',
    'password' = '',                               -- <DB_PASSWORD> (lokālajā PoC root bez paroles)
    'sink.buffer-flush.max-rows' = '64000',
    'sink.buffer-flush.interval-ms' = '3000'
);

INSERT INTO sr_crypto_trades
SELECT
    `data`.`s`,
    `data`.`t`,
    `data`.`p`,
    `data`.`q`,
    CAST(`data`.`p` * `data`.`q` AS DECIMAL(24,8)),
    CAST(TO_TIMESTAMP_LTZ(`data`.`T`, 3) AS TIMESTAMP(3)),
    `data`.`m`
FROM flink_binance_source;

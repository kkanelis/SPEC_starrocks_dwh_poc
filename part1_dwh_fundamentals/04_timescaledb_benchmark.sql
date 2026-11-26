\set ON_ERROR_STOP on
SET client_min_messages = warning;
SELECT version();

SELECT 'CREATE DATABASE poc_dwh_bench'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'poc_dwh_bench') \gexec
\c poc_dwh_bench

CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';

\timing on

DROP TABLE IF EXISTS orders CASCADE;
CREATE TABLE orders (
    order_id      BIGINT        NOT NULL,
    order_date    TIMESTAMP     NOT NULL,
    customer_id   INT,
    product_id    INT,
    total_price   NUMERIC(10,2),
    status        VARCHAR(20),
    delivery_date DATE,
    PRIMARY KEY (order_id, order_date)
);

SELECT create_hypertable('orders', by_range('order_date', INTERVAL '1 month'));

\copy orders (order_id, customer_id, product_id, total_price, status, order_date, delivery_date) FROM '/tmp/orders_5m.csv' WITH (FORMAT csv, HEADER true)

ANALYZE orders;

SELECT COUNT(*) AS total_rows FROM orders;
SELECT COUNT(*) AS chunks FROM timescaledb_information.chunks WHERE hypertable_name = 'orders';

EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*) AS cnt, SUM(total_price) AS revenue FROM orders WHERE delivery_date = '2026-06-15';

SELECT COUNT(*) AS cnt, SUM(total_price) AS revenue FROM orders WHERE delivery_date = '2026-06-15';
SELECT COUNT(*) AS cnt, SUM(total_price) AS revenue FROM orders WHERE delivery_date = '2026-06-15';
SELECT COUNT(*) AS cnt, SUM(total_price) AS revenue FROM orders WHERE delivery_date = '2026-06-15';
SELECT COUNT(*) AS cnt, SUM(total_price) AS revenue FROM orders WHERE delivery_date = '2026-06-15';
SELECT COUNT(*) AS cnt, SUM(total_price) AS revenue FROM orders WHERE delivery_date = '2026-06-15';

CREATE INDEX idx_orders_delivery_date ON orders (delivery_date);
ANALYZE orders;

EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*) AS cnt, SUM(total_price) AS revenue FROM orders WHERE delivery_date = '2026-06-15';

SELECT COUNT(*) AS cnt, SUM(total_price) AS revenue FROM orders WHERE delivery_date = '2026-06-15';
SELECT COUNT(*) AS cnt, SUM(total_price) AS revenue FROM orders WHERE delivery_date = '2026-06-15';
SELECT COUNT(*) AS cnt, SUM(total_price) AS revenue FROM orders WHERE delivery_date = '2026-06-15';
SELECT COUNT(*) AS cnt, SUM(total_price) AS revenue FROM orders WHERE delivery_date = '2026-06-15';
SELECT COUNT(*) AS cnt, SUM(total_price) AS revenue FROM orders WHERE delivery_date = '2026-06-15';

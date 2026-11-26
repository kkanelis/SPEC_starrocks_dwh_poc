USE poc_dwh;

EXPLAIN ANALYZE
SELECT COUNT(*) AS cnt, SUM(total_price) AS revenue
FROM orders
WHERE delivery_date = '2026-06-15';

EXPLAIN ANALYZE
SELECT COUNT(*) AS cnt, SUM(total_price) AS revenue
FROM orders
WHERE status = 'CANCELLED';

DROP MATERIALIZED VIEW IF EXISTS mv_daily_deliveries;

CREATE MATERIALIZED VIEW mv_daily_deliveries
DISTRIBUTED BY HASH(delivery_date)
REFRESH ASYNC
PROPERTIES ("replication_num" = "1")
AS
SELECT
    delivery_date,
    COUNT(*)         AS deliveries,
    SUM(total_price) AS revenue
FROM orders
WHERE status = 'DELIVERED'
GROUP BY delivery_date;

REFRESH MATERIALIZED VIEW mv_daily_deliveries WITH SYNC MODE;

SHOW CREATE MATERIALIZED VIEW mv_daily_deliveries;

SELECT COUNT(*) AS mv_rows FROM mv_daily_deliveries;
SELECT * FROM mv_daily_deliveries ORDER BY delivery_date DESC LIMIT 5;

SELECT (SELECT COUNT(*) FROM mv_daily_deliveries)                   AS mv_row_count,
       (SELECT SUM(deliveries) FROM mv_daily_deliveries)            AS mv_total,
       (SELECT COUNT(*) FROM orders WHERE status = 'DELIVERED')     AS orders_delivered;

SHOW MATERIALIZED VIEWS FROM poc_dwh WHERE name = 'mv_daily_deliveries'\G

SELECT task_name, state, create_time, finish_time
FROM information_schema.task_runs
WHERE `database` = 'poc_dwh'
ORDER BY create_time DESC
LIMIT 5;

SELECT order_id, order_date, customer_id, product_id, total_price, status, delivery_date
FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) = 1
LIMIT 5;

EXPLAIN ANALYZE
SELECT order_id, order_date, customer_id, product_id, total_price, status, delivery_date
FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) = 1;

EXPLAIN ANALYZE
SELECT order_id, order_date, customer_id, product_id, total_price, status, delivery_date
FROM (
    SELECT order_id, order_date, customer_id, product_id, total_price, status, delivery_date,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
    FROM orders
) t
WHERE rn = 1;

SELECT COUNT(DISTINCT customer_id) AS distinct_customers FROM orders;

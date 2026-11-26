CREATE DATABASE IF NOT EXISTS poc_dwh;
USE poc_dwh;

DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id      BIGINT        NOT NULL,
    order_date    DATETIME      NOT NULL,
    customer_id   INT,
    product_id    INT,
    total_price   DECIMAL(10, 2),
    status        VARCHAR(20),
    delivery_date DATE,
    INDEX idx_status (status) USING BITMAP,
    INDEX idx_delivery_date (delivery_date) USING BITMAP
)
ENGINE = OLAP
PRIMARY KEY (order_id, order_date)
PARTITION BY date_trunc('month', order_date)
DISTRIBUTED BY HASH(order_id)
ORDER BY (delivery_date, customer_id)
PROPERTIES (
    "replication_num" = "1",
    "enable_persistent_index" = "true"
);

SHOW CREATE TABLE orders;

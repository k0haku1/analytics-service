CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.orders
(
    order_id    UUID,
    customer_id UUID,
    product_id UUID,
    product_name String,
    quantity Int32,
    event_key   String
)
    ENGINE = MergeTree
    ORDER BY (order_id, customer_id);
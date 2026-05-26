CREATE KEYSPACE IF NOT EXISTS reports
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE reports;

CREATE TABLE IF NOT EXISTS product_sales (
    product_id     bigint PRIMARY KEY,
    product_name   text,
    category       text,
    total_quantity bigint,
    total_revenue  decimal,
    sales_count    bigint,
    avg_rating     decimal,
    total_reviews  bigint
);

CREATE TABLE IF NOT EXISTS customer_sales (
    customer_id  bigint PRIMARY KEY,
    full_name    text,
    email        text,
    country      text,
    orders_count bigint,
    total_spent  decimal,
    avg_check    decimal
);

CREATE TABLE IF NOT EXISTS time_sales (
    year           int,
    month          int,
    orders_count   bigint,
    total_quantity bigint,
    total_revenue  decimal,
    avg_order      decimal,
    PRIMARY KEY (year, month)
);

CREATE TABLE IF NOT EXISTS store_sales (
    store_id      bigint PRIMARY KEY,
    store_name    text,
    city          text,
    state         text,
    country       text,
    orders_count  bigint,
    total_revenue decimal,
    avg_check     decimal
);

CREATE TABLE IF NOT EXISTS supplier_sales (
    supplier_id       bigint PRIMARY KEY,
    supplier_name     text,
    country           text,
    orders_count      bigint,
    total_quantity    bigint,
    total_revenue     decimal,
    avg_product_price decimal
);

CREATE TABLE IF NOT EXISTS product_quality (
    product_id          bigint PRIMARY KEY,
    product_name        text,
    rating              decimal,
    reviews             bigint,
    total_quantity_sold bigint,
    total_revenue       decimal
);

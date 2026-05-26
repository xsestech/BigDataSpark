CREATE DATABASE IF NOT EXISTS reports;

DROP TABLE IF EXISTS reports.product_sales;
CREATE TABLE reports.product_sales (
    product_id      Int64,
    product_name    String,
    category        String,
    total_quantity  Int64,
    total_revenue   Decimal(18, 2),
    sales_count     Int64,
    avg_rating      Decimal(5, 2),
    total_reviews   Int64
) ENGINE = MergeTree() ORDER BY product_id;

DROP TABLE IF EXISTS reports.customer_sales;
CREATE TABLE reports.customer_sales (
    customer_id   Int64,
    full_name     String,
    email         String,
    country       String,
    orders_count  Int64,
    total_spent   Decimal(18, 2),
    avg_check     Decimal(18, 2)
) ENGINE = MergeTree() ORDER BY customer_id;

DROP TABLE IF EXISTS reports.time_sales;
CREATE TABLE reports.time_sales (
    year           Int32,
    month          Int32,
    orders_count   Int64,
    total_quantity Int64,
    total_revenue  Decimal(18, 2),
    avg_order      Decimal(18, 2)
) ENGINE = MergeTree() ORDER BY (year, month);

DROP TABLE IF EXISTS reports.store_sales;
CREATE TABLE reports.store_sales (
    store_id       Int64,
    store_name     String,
    city           String,
    state          String,
    country        String,
    orders_count   Int64,
    total_revenue  Decimal(18, 2),
    avg_check      Decimal(18, 2)
) ENGINE = MergeTree() ORDER BY store_id;

DROP TABLE IF EXISTS reports.supplier_sales;
CREATE TABLE reports.supplier_sales (
    supplier_id        Int64,
    supplier_name      String,
    country            String,
    orders_count       Int64,
    total_quantity     Int64,
    total_revenue      Decimal(18, 2),
    avg_product_price  Decimal(18, 2)
) ENGINE = MergeTree() ORDER BY supplier_id;

DROP TABLE IF EXISTS reports.product_quality;
CREATE TABLE reports.product_quality (
    product_id           Int64,
    product_name         String,
    rating               Decimal(5, 2),
    reviews              Int64,
    total_quantity_sold  Int64,
    total_revenue        Decimal(18, 2)
) ENGINE = MergeTree() ORDER BY product_id;

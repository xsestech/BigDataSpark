DROP TABLE IF EXISTS fact_sales           CASCADE;
DROP TABLE IF EXISTS dim_product          CASCADE;
DROP TABLE IF EXISTS dim_store            CASCADE;
DROP TABLE IF EXISTS dim_seller           CASCADE;
DROP TABLE IF EXISTS dim_customer         CASCADE;
DROP TABLE IF EXISTS dim_supplier         CASCADE;
DROP TABLE IF EXISTS dim_brand            CASCADE;
DROP TABLE IF EXISTS dim_product_category CASCADE;
DROP TABLE IF EXISTS dim_location         CASCADE;
DROP TABLE IF EXISTS dim_pet              CASCADE;
DROP TABLE IF EXISTS dim_pet_breed        CASCADE;
DROP TABLE IF EXISTS dim_pet_category     CASCADE;


CREATE TABLE dim_pet_category (
    pet_category_id BIGINT       PRIMARY KEY,
    pet_type        VARCHAR(64)  NOT NULL,
    category_name   VARCHAR(64)  NOT NULL
);

CREATE TABLE dim_pet_breed (
    breed_id        BIGINT       PRIMARY KEY,
    breed_name      VARCHAR(128) NOT NULL,
    pet_category_id BIGINT       NOT NULL REFERENCES dim_pet_category(pet_category_id),
    UNIQUE (breed_name, pet_category_id)
);

CREATE TABLE dim_pet (
    pet_id     BIGINT       PRIMARY KEY,
    pet_name   VARCHAR(128) NOT NULL,
    breed_id   BIGINT       NOT NULL REFERENCES dim_pet_breed(breed_id),
    UNIQUE (pet_name, breed_id)
);

CREATE TABLE dim_location (
    location_id  BIGINT       PRIMARY KEY,
    country      VARCHAR(128) NOT NULL,
    state        VARCHAR(128),
    city         VARCHAR(128),
    postal_code  VARCHAR(32),
    UNIQUE (country, state, city, postal_code)
);

CREATE TABLE dim_product_category (
    product_category_id BIGINT       PRIMARY KEY,
    category_name       VARCHAR(128) NOT NULL UNIQUE
);

CREATE TABLE dim_brand (
    brand_id    BIGINT       PRIMARY KEY,
    brand_name  VARCHAR(128) NOT NULL UNIQUE
);

CREATE TABLE dim_supplier (
    supplier_id   BIGINT       PRIMARY KEY,
    supplier_name VARCHAR(255) NOT NULL,
    contact_name  VARCHAR(255),
    email         VARCHAR(255),
    phone         VARCHAR(64),
    address       VARCHAR(255),
    location_id   BIGINT       NOT NULL REFERENCES dim_location(location_id),
    UNIQUE (supplier_name, email)
);

CREATE TABLE dim_customer (
    customer_id  BIGINT       PRIMARY KEY,
    first_name   VARCHAR(255) NOT NULL,
    last_name    VARCHAR(255) NOT NULL,
    age          INT          CONSTRAINT valid_age CHECK (age >= 0 AND age < 200),
    email        VARCHAR(255) NOT NULL UNIQUE,
    location_id  BIGINT       NOT NULL REFERENCES dim_location(location_id),
    pet_id       BIGINT       REFERENCES dim_pet(pet_id)
);

CREATE TABLE dim_seller (
    seller_id    BIGINT       PRIMARY KEY,
    first_name   VARCHAR(255) NOT NULL,
    last_name    VARCHAR(255) NOT NULL,
    email        VARCHAR(255) NOT NULL UNIQUE,
    location_id  BIGINT       NOT NULL REFERENCES dim_location(location_id)
);

CREATE TABLE dim_store (
    store_id        BIGINT       PRIMARY KEY,
    store_name      VARCHAR(255) NOT NULL,
    store_location  VARCHAR(255),
    phone           VARCHAR(64),
    email           VARCHAR(255),
    location_id     BIGINT       NOT NULL REFERENCES dim_location(location_id),
    UNIQUE (store_name, location_id)
);

CREATE TABLE dim_product (
    product_id          BIGINT        PRIMARY KEY,
    product_name        VARCHAR(255)  NOT NULL,
    description         TEXT,
    price               NUMERIC(12,2) CONSTRAINT valid_price  CHECK (price  >= 0),
    weight              NUMERIC(10,3) CONSTRAINT valid_weight CHECK (weight >= 0),
    color               VARCHAR(64),
    size                VARCHAR(32),
    material            VARCHAR(128),
    rating              NUMERIC(3,2)  CONSTRAINT valid_rating CHECK (rating BETWEEN 0 AND 5),
    reviews             INT           CONSTRAINT valid_reviews CHECK (reviews >= 0),
    release_date        DATE,
    expiry_date         DATE,
    product_category_id BIGINT NOT NULL REFERENCES dim_product_category(product_category_id),
    brand_id            BIGINT NOT NULL REFERENCES dim_brand(brand_id),
    supplier_id         BIGINT NOT NULL REFERENCES dim_supplier(supplier_id),
    UNIQUE (product_name, brand_id)
);

CREATE TABLE fact_sales (
    sale_id      BIGINT        PRIMARY KEY,
    sale_date    DATE          NOT NULL,
    customer_id  BIGINT        NOT NULL REFERENCES dim_customer(customer_id),
    seller_id    BIGINT        NOT NULL REFERENCES dim_seller(seller_id),
    product_id   BIGINT        NOT NULL REFERENCES dim_product(product_id),
    store_id     BIGINT        NOT NULL REFERENCES dim_store(store_id),
    quantity     INT           NOT NULL CONSTRAINT valid_quantity    CHECK (quantity > 0),
    total_price  NUMERIC(12,2) NOT NULL CONSTRAINT valid_total_price CHECK (total_price >= 0)
);

CREATE INDEX idx_fact_sales_date     ON fact_sales (sale_date);
CREATE INDEX idx_fact_sales_customer ON fact_sales (customer_id);
CREATE INDEX idx_fact_sales_seller   ON fact_sales (seller_id);
CREATE INDEX idx_fact_sales_product  ON fact_sales (product_id);
CREATE INDEX idx_fact_sales_store    ON fact_sales (store_id);

from __future__ import annotations

import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from jobs.reports_common import build_spark

PG_URL = os.environ["PG_URL"]
PG_PROPS = {
    "user": os.environ["PG_USER"],
    "password": os.environ["PG_PASSWORD"],
    "driver": "org.postgresql.Driver",
}

TABLES_PARENT_FIRST = [
    "dim_pet_category",
    "dim_pet_breed",
    "dim_pet",
    "dim_location",
    "dim_product_category",
    "dim_brand",
    "dim_supplier",
    "dim_customer",
    "dim_seller",
    "dim_store",
    "dim_product",
    "fact_sales",
]


def cast_raw(raw: DataFrame) -> DataFrame:
    return (
        raw.withColumn("customer_age", F.col("customer_age").cast("int"))
        .withColumn("product_price", F.col("product_price").cast("decimal(12,2)"))
        .withColumn("product_quantity", F.col("product_quantity").cast("int"))
        .withColumn("product_weight", F.col("product_weight").cast("decimal(10,3)"))
        .withColumn("product_rating", F.col("product_rating").cast("decimal(3,2)"))
        .withColumn("product_reviews", F.col("product_reviews").cast("int"))
        .withColumn("sale_quantity", F.col("sale_quantity").cast("int"))
        .withColumn("sale_total_price", F.col("sale_total_price").cast("decimal(14,2)"))
        .withColumn("sale_date", F.to_date("sale_date", "M/d/yyyy"))
        .withColumn(
            "product_release_date", F.to_date("product_release_date", "M/d/yyyy")
        )
        .withColumn("product_expiry_date", F.to_date("product_expiry_date", "M/d/yyyy"))
    )


def with_surrogate_key(df: DataFrame, id_name: str) -> DataFrame:
    return df.withColumn(id_name, F.monotonically_increasing_id())


def truncate_all(spark: SparkSession) -> None:
    jvm = spark.sparkContext._jvm
    jvm.org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry.register(
        PG_PROPS["driver"]
    )
    conn = jvm.java.sql.DriverManager.getConnection(
        PG_URL, PG_PROPS["user"], PG_PROPS["password"]
    )
    try:
        stmt = conn.createStatement()
        stmt.execute("TRUNCATE TABLE " + ", ".join(TABLES_PARENT_FIRST) + " CASCADE")
        stmt.close()
    finally:
        conn.close()


def write(df: DataFrame, table: str) -> None:
    (df.write.mode("append").jdbc(PG_URL, table, properties=PG_PROPS))
    print(f"wrote {table}: {df.count()} rows")


def main() -> None:
    spark = build_spark("etl_snowflake")

    raw = cast_raw(spark.read.jdbc(PG_URL, "raw_sales", properties=PG_PROPS))
    raw.cache()
    print(f"raw_sales rows: {raw.count()}")

    dim_pet_category = (
        raw.select(
            F.lower(F.col("customer_pet_type")).alias("pet_type"),
            F.col("pet_category").alias("category_name"),
        )
        .where(F.col("pet_type").isNotNull())
        .dropDuplicates(["pet_type"])
    )
    dim_pet_category = with_surrogate_key(dim_pet_category, "pet_category_id")
    dim_pet_category.cache()

    dim_pet_breed = (
        raw.select(
            F.col("customer_pet_breed").alias("breed_name"),
            F.lower(F.col("customer_pet_type")).alias("pet_type"),
        )
        .where(F.col("breed_name").isNotNull())
        .join(dim_pet_category.select("pet_category_id", "pet_type"), "pet_type")
        .select("breed_name", "pet_category_id")
        .dropDuplicates(["breed_name", "pet_category_id"])
    )
    dim_pet_breed = with_surrogate_key(dim_pet_breed, "breed_id")
    dim_pet_breed.cache()

    dim_pet = (
        raw.select(
            F.col("customer_pet_name").alias("pet_name"),
            F.col("customer_pet_breed").alias("breed_name"),
            F.lower(F.col("customer_pet_type")).alias("pet_type"),
        )
        .where(F.col("pet_name").isNotNull())
        .join(dim_pet_category.select("pet_category_id", "pet_type"), "pet_type")
        .join(
            dim_pet_breed.select("breed_id", "breed_name", "pet_category_id"),
            ["breed_name", "pet_category_id"],
        )
        .select("pet_name", "breed_id")
        .dropDuplicates(["pet_name", "breed_id"])
    )
    dim_pet = with_surrogate_key(dim_pet, "pet_id")
    dim_pet.cache()

    loc_customer = raw.select(
        F.col("customer_country").alias("country"),
        F.lit(None).cast("string").alias("state"),
        F.lit(None).cast("string").alias("city"),
        F.col("customer_postal_code").alias("postal_code"),
    ).where(F.col("customer_country").isNotNull())
    loc_seller = raw.select(
        F.col("seller_country").alias("country"),
        F.lit(None).cast("string").alias("state"),
        F.lit(None).cast("string").alias("city"),
        F.col("seller_postal_code").alias("postal_code"),
    ).where(F.col("seller_country").isNotNull())
    loc_store = raw.select(
        F.col("store_country").alias("country"),
        F.col("store_state").alias("state"),
        F.col("store_city").alias("city"),
        F.lit(None).cast("string").alias("postal_code"),
    ).where(F.col("store_country").isNotNull())
    loc_supplier = raw.select(
        F.col("supplier_country").alias("country"),
        F.lit(None).cast("string").alias("state"),
        F.col("supplier_city").alias("city"),
        F.lit(None).cast("string").alias("postal_code"),
    ).where(F.col("supplier_country").isNotNull())

    dim_location = (
        loc_customer.union(loc_seller).union(loc_store).union(loc_supplier).distinct()
    )
    dim_location = with_surrogate_key(dim_location, "location_id")
    dim_location.cache()

    dim_product_category = (
        raw.select(F.col("product_category").alias("category_name"))
        .where(F.col("category_name").isNotNull())
        .dropDuplicates(["category_name"])
    )
    dim_product_category = with_surrogate_key(
        dim_product_category, "product_category_id"
    )
    dim_product_category.cache()

    dim_brand = (
        raw.select(F.col("product_brand").alias("brand_name"))
        .where(F.col("brand_name").isNotNull())
        .dropDuplicates(["brand_name"])
    )
    dim_brand = with_surrogate_key(dim_brand, "brand_id")
    dim_brand.cache()

    dim_supplier = (
        raw.alias("r")
        .join(
            dim_location.alias("l"),
            (F.col("l.country") == F.col("r.supplier_country"))
            & F.col("l.city").eqNullSafe(F.col("r.supplier_city"))
            & F.col("l.state").isNull()
            & F.col("l.postal_code").isNull(),
        )
        .where(F.col("r.supplier_name").isNotNull())
        .select(
            F.col("r.supplier_name").alias("supplier_name"),
            F.col("r.supplier_contact").alias("contact_name"),
            F.col("r.supplier_email").alias("email"),
            F.col("r.supplier_phone").alias("phone"),
            F.col("r.supplier_address").alias("address"),
            F.col("l.location_id").alias("location_id"),
        )
        .dropDuplicates(["supplier_name", "email"])
    )
    dim_supplier = with_surrogate_key(dim_supplier, "supplier_id")
    dim_supplier.cache()

    dim_customer = (
        raw.alias("r")
        .join(
            dim_location.alias("l"),
            (F.col("l.country") == F.col("r.customer_country"))
            & F.col("l.postal_code").eqNullSafe(F.col("r.customer_postal_code"))
            & F.col("l.state").isNull()
            & F.col("l.city").isNull(),
        )
        .join(
            dim_pet_category.alias("pc"),
            F.col("pc.pet_type") == F.lower(F.col("r.customer_pet_type")),
            "left",
        )
        .join(
            dim_pet_breed.alias("pb"),
            (F.col("pb.breed_name") == F.col("r.customer_pet_breed"))
            & (F.col("pb.pet_category_id") == F.col("pc.pet_category_id")),
            "left",
        )
        .join(
            dim_pet.alias("p"),
            (F.col("p.pet_name") == F.col("r.customer_pet_name"))
            & (F.col("p.breed_id") == F.col("pb.breed_id")),
            "left",
        )
        .where(F.col("r.customer_email").isNotNull())
        .select(
            F.col("r.customer_first_name").alias("first_name"),
            F.col("r.customer_last_name").alias("last_name"),
            F.col("r.customer_age").alias("age"),
            F.col("r.customer_email").alias("email"),
            F.col("l.location_id").alias("location_id"),
            F.col("p.pet_id").alias("pet_id"),
        )
        .dropDuplicates(["email"])
    )
    dim_customer = with_surrogate_key(dim_customer, "customer_id")
    dim_customer.cache()

    dim_seller = (
        raw.alias("r")
        .join(
            dim_location.alias("l"),
            (F.col("l.country") == F.col("r.seller_country"))
            & F.col("l.postal_code").eqNullSafe(F.col("r.seller_postal_code"))
            & F.col("l.state").isNull()
            & F.col("l.city").isNull(),
        )
        .where(F.col("r.seller_email").isNotNull())
        .select(
            F.col("r.seller_first_name").alias("first_name"),
            F.col("r.seller_last_name").alias("last_name"),
            F.col("r.seller_email").alias("email"),
            F.col("l.location_id").alias("location_id"),
        )
        .dropDuplicates(["email"])
    )
    dim_seller = with_surrogate_key(dim_seller, "seller_id")
    dim_seller.cache()

    dim_store = (
        raw.alias("r")
        .join(
            dim_location.alias("l"),
            (F.col("l.country") == F.col("r.store_country"))
            & F.col("l.state").eqNullSafe(F.col("r.store_state"))
            & F.col("l.city").eqNullSafe(F.col("r.store_city"))
            & F.col("l.postal_code").isNull(),
        )
        .where(F.col("r.store_name").isNotNull())
        .select(
            F.col("r.store_name").alias("store_name"),
            F.col("r.store_location").alias("store_location"),
            F.col("r.store_phone").alias("phone"),
            F.col("r.store_email").alias("email"),
            F.col("l.location_id").alias("location_id"),
        )
        .dropDuplicates(["store_name", "location_id"])
    )
    dim_store = with_surrogate_key(dim_store, "store_id")
    dim_store.cache()

    dim_product = (
        raw.alias("r")
        .join(
            dim_product_category.alias("pc"),
            F.col("pc.category_name") == F.col("r.product_category"),
        )
        .join(dim_brand.alias("b"), F.col("b.brand_name") == F.col("r.product_brand"))
        .join(
            dim_supplier.alias("s"),
            (F.col("s.supplier_name") == F.col("r.supplier_name"))
            & F.col("s.email").eqNullSafe(F.col("r.supplier_email")),
        )
        .where(F.col("r.product_name").isNotNull())
        .select(
            F.col("r.product_name").alias("product_name"),
            F.col("r.product_description").alias("description"),
            F.col("r.product_price").alias("price"),
            F.col("r.product_weight").alias("weight"),
            F.col("r.product_color").alias("color"),
            F.col("r.product_size").alias("size"),
            F.col("r.product_material").alias("material"),
            F.col("r.product_rating").alias("rating"),
            F.col("r.product_reviews").alias("reviews"),
            F.col("r.product_release_date").alias("release_date"),
            F.col("r.product_expiry_date").alias("expiry_date"),
            F.col("pc.product_category_id").alias("product_category_id"),
            F.col("b.brand_id").alias("brand_id"),
            F.col("s.supplier_id").alias("supplier_id"),
        )
        .dropDuplicates(["product_name", "brand_id"])
    )
    dim_product = with_surrogate_key(dim_product, "product_id")
    dim_product.cache()

    fact_sales = (
        raw.alias("r")
        .join(
            dim_customer.select("customer_id", F.col("email").alias("c_email")).alias(
                "c"
            ),
            F.col("r.customer_email") == F.col("c.c_email"),
        )
        .join(
            dim_seller.select("seller_id", F.col("email").alias("s_email")).alias("sl"),
            F.col("r.seller_email") == F.col("sl.s_email"),
        )
        .join(dim_brand.alias("b"), F.col("b.brand_name") == F.col("r.product_brand"))
        .join(
            dim_product.select("product_id", "product_name", "brand_id").alias("p"),
            (F.col("p.product_name") == F.col("r.product_name"))
            & (F.col("p.brand_id") == F.col("b.brand_id")),
        )
        .join(
            dim_location.alias("ls"),
            (F.col("ls.country") == F.col("r.store_country"))
            & F.col("ls.state").eqNullSafe(F.col("r.store_state"))
            & F.col("ls.city").eqNullSafe(F.col("r.store_city"))
            & F.col("ls.postal_code").isNull(),
        )
        .join(
            dim_store.select(
                "store_id",
                F.col("store_name").alias("st_name"),
                F.col("location_id").alias("st_loc"),
            ).alias("st"),
            (F.col("st.st_name") == F.col("r.store_name"))
            & (F.col("st.st_loc") == F.col("ls.location_id")),
        )
        .select(
            F.col("r.sale_date").alias("sale_date"),
            F.col("c.customer_id"),
            F.col("sl.seller_id"),
            F.col("p.product_id"),
            F.col("st.store_id"),
            F.col("r.sale_quantity").alias("quantity"),
            F.col("r.sale_total_price").alias("total_price"),
        )
    )
    fact_sales = with_surrogate_key(fact_sales, "sale_id")

    truncate_all(spark)

    write(dim_pet_category, "dim_pet_category")
    write(dim_pet_breed, "dim_pet_breed")
    write(dim_pet, "dim_pet")
    write(dim_location, "dim_location")
    write(dim_product_category, "dim_product_category")
    write(dim_brand, "dim_brand")
    write(dim_supplier, "dim_supplier")
    write(dim_customer, "dim_customer")
    write(dim_seller, "dim_seller")
    write(dim_store, "dim_store")
    write(dim_product, "dim_product")
    write(fact_sales, "fact_sales")

    spark.stop()


if __name__ == "__main__":
    main()

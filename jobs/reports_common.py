import os
from typing import Dict, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

PG_URL = os.environ["PG_URL"]
PG_PROPS = {
    "user": os.environ["PG_USER"],
    "password": os.environ["PG_PASSWORD"],
    "driver": "org.postgresql.Driver",
}

SPARK_MEMORY_CAP = os.environ.get("SPARK_MEMORY_CAP", "2g")


def build_spark(
    app_name: str, extra_conf: Optional[Dict[str, str]] = None
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.executor.memory", SPARK_MEMORY_CAP)
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_pg(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.jdbc(PG_URL, table, properties=PG_PROPS)


def build_reports(spark: SparkSession) -> Dict[str, DataFrame]:
    fact = read_pg(spark, "fact_sales")
    dim_product = read_pg(spark, "dim_product")
    dim_product_category = read_pg(spark, "dim_product_category")
    dim_customer = read_pg(spark, "dim_customer")
    dim_store = read_pg(spark, "dim_store")
    dim_supplier = read_pg(spark, "dim_supplier")
    dim_location = read_pg(spark, "dim_location")

    product_sales = (
        fact.groupBy("product_id")
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("total_price").alias("total_revenue"),
            F.count("*").alias("sales_count"),
        )
        .join(
            dim_product.select(
                "product_id", "product_name", "product_category_id", "rating", "reviews"
            ),
            "product_id",
        )
        .join(
            dim_product_category.select(
                "product_category_id", F.col("category_name").alias("category")
            ),
            "product_category_id",
            "left",
        )
        .select(
            F.col("product_id").cast("long"),
            F.coalesce(F.col("product_name"), F.lit("")).alias("product_name"),
            F.coalesce(F.col("category"), F.lit("")).alias("category"),
            F.col("total_quantity").cast("long"),
            F.col("total_revenue").cast("decimal(18,2)"),
            F.col("sales_count").cast("long"),
            F.coalesce(F.col("rating"), F.lit(0))
            .cast("decimal(5,2)")
            .alias("avg_rating"),
            F.coalesce(F.col("reviews"), F.lit(0)).cast("long").alias("total_reviews"),
        )
    )

    customer_sales = (
        fact.groupBy("customer_id")
        .agg(
            F.count("*").alias("orders_count"),
            F.sum("total_price").alias("total_spent"),
            F.avg("total_price").alias("avg_check"),
        )
        .join(
            dim_customer.select(
                "customer_id", "first_name", "last_name", "email", "location_id"
            ),
            "customer_id",
        )
        .join(dim_location.select("location_id", "country"), "location_id", "left")
        .select(
            F.col("customer_id").cast("long"),
            F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias(
                "full_name"
            ),
            F.coalesce(F.col("email"), F.lit("")).alias("email"),
            F.coalesce(F.col("country"), F.lit("")).alias("country"),
            F.col("orders_count").cast("long"),
            F.col("total_spent").cast("decimal(18,2)"),
            F.col("avg_check").cast("decimal(18,2)"),
        )
    )

    time_sales = (
        fact.withColumn("year", F.year("sale_date"))
        .withColumn("month", F.month("sale_date"))
        .groupBy("year", "month")
        .agg(
            F.count("*").alias("orders_count"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("total_price").alias("total_revenue"),
            F.avg("total_price").alias("avg_order"),
        )
        .select(
            F.col("year").cast("int"),
            F.col("month").cast("int"),
            F.col("orders_count").cast("long"),
            F.col("total_quantity").cast("long"),
            F.col("total_revenue").cast("decimal(18,2)"),
            F.col("avg_order").cast("decimal(18,2)"),
        )
    )

    store_sales = (
        fact.groupBy("store_id")
        .agg(
            F.count("*").alias("orders_count"),
            F.sum("total_price").alias("total_revenue"),
            F.avg("total_price").alias("avg_check"),
        )
        .join(
            dim_store.select("store_id", "store_name", "location_id"),
            "store_id",
        )
        .join(
            dim_location.select("location_id", "city", "state", "country"),
            "location_id",
            "left",
        )
        .select(
            F.col("store_id").cast("long"),
            F.coalesce(F.col("store_name"), F.lit("")).alias("store_name"),
            F.coalesce(F.col("city"), F.lit("")).alias("city"),
            F.coalesce(F.col("state"), F.lit("")).alias("state"),
            F.coalesce(F.col("country"), F.lit("")).alias("country"),
            F.col("orders_count").cast("long"),
            F.col("total_revenue").cast("decimal(18,2)"),
            F.col("avg_check").cast("decimal(18,2)"),
        )
    )

    supplier_sales = (
        fact.join(
            dim_product.select("product_id", "supplier_id", "price"), "product_id"
        )
        .groupBy("supplier_id")
        .agg(
            F.count("*").alias("orders_count"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("total_price").alias("total_revenue"),
            F.avg("price").alias("avg_product_price"),
        )
        .join(
            dim_supplier.select("supplier_id", "supplier_name", "location_id"),
            "supplier_id",
        )
        .join(dim_location.select("location_id", "country"), "location_id", "left")
        .select(
            F.col("supplier_id").cast("long"),
            F.coalesce(F.col("supplier_name"), F.lit("")).alias("supplier_name"),
            F.coalesce(F.col("country"), F.lit("")).alias("country"),
            F.col("orders_count").cast("long"),
            F.col("total_quantity").cast("long"),
            F.col("total_revenue").cast("decimal(18,2)"),
            F.col("avg_product_price").cast("decimal(18,2)"),
        )
    )

    product_quality = (
        fact.groupBy("product_id")
        .agg(
            F.sum("quantity").alias("total_quantity_sold"),
            F.sum("total_price").alias("total_revenue"),
        )
        .join(
            dim_product.select("product_id", "product_name", "rating", "reviews"),
            "product_id",
        )
        .select(
            F.col("product_id").cast("long"),
            F.coalesce(F.col("product_name"), F.lit("")).alias("product_name"),
            F.coalesce(F.col("rating"), F.lit(0)).cast("decimal(5,2)").alias("rating"),
            F.coalesce(F.col("reviews"), F.lit(0)).cast("long").alias("reviews"),
            F.col("total_quantity_sold").cast("long"),
            F.col("total_revenue").cast("decimal(18,2)"),
        )
    )

    return {
        "product_sales": product_sales,
        "customer_sales": customer_sales,
        "time_sales": time_sales,
        "store_sales": store_sales,
        "supplier_sales": supplier_sales,
        "product_quality": product_quality,
    }

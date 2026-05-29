import os

from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType

from jobs.reports_common import build_reports, build_spark


def decimals_to_double(df: DataFrame) -> DataFrame:
    for f in df.schema.fields:
        if isinstance(f.dataType, DecimalType):
            df = df.withColumn(f.name, df[f.name].cast("double"))
    return df


NEO4J_URL = os.environ["NEO4J_URL"]
NEO4J_USER = os.environ["NEO4J_USER"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]

TARGETS = {
    "product_sales": (":ProductSale", "product_id"),
    "customer_sales": (":CustomerSale", "customer_id"),
    "time_sales": (":TimeSale", "year,month"),
    "store_sales": (":StoreSale", "store_id"),
    "supplier_sales": (":SupplierSale", "supplier_id"),
    "product_quality": (":ProductQuality", "product_id"),
}


def main() -> None:
    spark = build_spark("etl_reports_neo4j")

    for name, df in build_reports(spark).items():
        label, keys = TARGETS[name]
        df = decimals_to_double(df)
        (
            df.write.format("org.neo4j.spark.DataSource")
            .mode("Overwrite")
            .option("url", NEO4J_URL)
            .option("authentication.type", "basic")
            .option("authentication.basic.username", NEO4J_USER)
            .option("authentication.basic.password", NEO4J_PASSWORD)
            .option("labels", label)
            .option("node.keys", keys)
            .save()
        )
        print(f"wrote {label}: {df.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()

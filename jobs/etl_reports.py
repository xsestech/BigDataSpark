import os

from pyspark.sql import SparkSession

from jobs.reports_common import build_reports

CH_HOST = os.environ.get("CH_HOST", "clickhouse")
CH_HTTP_PORT = os.environ.get("CH_HTTP_PORT", "8123")
CH_USER = os.environ["CH_USER"]
CH_PASSWORD = os.environ["CH_PASSWORD"]
CH_DB = os.environ.get("CH_DB", "reports")
CH_CATALOG = "ch"


def main() -> None:
    spark = (
        SparkSession.builder.appName("etl_reports_clickhouse")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            f"spark.sql.catalog.{CH_CATALOG}", "com.clickhouse.spark.ClickHouseCatalog"
        )
        .config(f"spark.sql.catalog.{CH_CATALOG}.host", CH_HOST)
        .config(f"spark.sql.catalog.{CH_CATALOG}.protocol", "http")
        .config(f"spark.sql.catalog.{CH_CATALOG}.http_port", CH_HTTP_PORT)
        .config(f"spark.sql.catalog.{CH_CATALOG}.user", CH_USER)
        .config(f"spark.sql.catalog.{CH_CATALOG}.password", CH_PASSWORD)
        .config(f"spark.sql.catalog.{CH_CATALOG}.database", CH_DB)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    for name, df in build_reports(spark).items():
        qualified = f"{CH_CATALOG}.{CH_DB}.{name}"
        spark.sql(f"TRUNCATE TABLE {qualified}")
        df.writeTo(qualified).using("clickhouse").append()
        print(f"wrote {qualified}: {df.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()

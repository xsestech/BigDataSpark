import os

from pyspark.sql import SparkSession

from jobs.reports_common import build_reports

MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB = os.environ.get("MONGO_DB", "reports")


def main() -> None:
    spark = (
        SparkSession.builder.appName("etl_reports_mongo")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    for name, df in build_reports(spark).items():
        (
            df.write.format("mongodb")
            .mode("overwrite")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DB)
            .option("collection", name)
            .save()
        )
        print(f"wrote {MONGO_DB}.{name}: {df.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()

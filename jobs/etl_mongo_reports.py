import os

from jobs.reports_common import build_reports, build_spark

MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB = os.environ.get("MONGO_DB", "reports")


def main() -> None:
    spark = build_spark("etl_reports_mongo")

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

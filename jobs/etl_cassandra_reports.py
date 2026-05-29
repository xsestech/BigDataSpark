import os

from jobs.reports_common import build_reports, build_spark

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.environ.get("CASSANDRA_PORT", "9042")
CASSANDRA_USER = os.environ["CASSANDRA_USER"]
CASSANDRA_PASSWORD = os.environ["CASSANDRA_PASSWORD"]
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "reports")


def main() -> None:
    spark = build_spark(
        "etl_reports_cassandra",
        {
            "spark.cassandra.connection.host": CASSANDRA_HOST,
            "spark.cassandra.connection.port": CASSANDRA_PORT,
            "spark.cassandra.auth.username": CASSANDRA_USER,
            "spark.cassandra.auth.password": CASSANDRA_PASSWORD,
            "spark.sql.extensions": "com.datastax.spark.connector.CassandraSparkExtensions",
            "spark.sql.catalog.cass": "com.datastax.spark.connector.datasource.CassandraCatalog",
        },
    )

    for name, df in build_reports(spark).items():
        (
            df.write.format("org.apache.spark.sql.cassandra")
            .options(
                keyspace=CASSANDRA_KEYSPACE, table=name, **{"confirm.truncate": "true"}
            )
            .mode("overwrite")
            .save()
        )
        print(f"wrote {CASSANDRA_KEYSPACE}.{name}: {df.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()

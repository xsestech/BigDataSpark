import os

import redis
from pyspark.sql import SparkSession

from jobs.reports_common import build_reports

VALKEY_HOST = os.environ.get("VALKEY_HOST", "valkey")
VALKEY_PORT = int(os.environ.get("VALKEY_PORT", "6379"))

PK_COLUMNS = {
    "product_sales": ("product_id",),
    "customer_sales": ("customer_id",),
    "time_sales": ("year", "month"),
    "store_sales": ("store_id",),
    "supplier_sales": ("supplier_id",),
    "product_quality": ("product_id",),
}


def make_pk(name, row):
    if name == "time_sales":
        return "{}:{:02d}".format(row["year"], row["month"])
    return str(row[PK_COLUMNS[name][0]])


def clear_report(client, name):
    for batch in _scan_batches(client, "{}:*".format(name)):
        if batch:
            client.delete(*batch)


def _scan_batches(client, pattern, count=1000):
    cursor = 0
    while True:
        cursor, keys = client.scan(cursor=cursor, match=pattern, count=count)
        yield keys
        if cursor == 0:
            break


def main():
    spark = (
        SparkSession.builder.appName("etl_reports_valkey")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    client = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, decode_responses=True)

    for name, df in build_reports(spark).items():
        clear_report(client, name)
        rows = df.collect()
        columns = df.columns
        pipe = client.pipeline(transaction=False)
        for row in rows:
            key = "{}:{}".format(name, make_pk(name, row))
            mapping = {c: ("" if row[c] is None else str(row[c])) for c in columns}
            pipe.hset(key, mapping=mapping)
        pipe.execute()
        print("wrote {}: {} keys".format(name, len(rows)))

    spark.stop()


if __name__ == "__main__":
    main()

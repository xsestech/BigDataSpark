#!/bin/sh
set -eu

for f in /data/MOCK_DATA_*.csv; do
    echo "Loading $f"
    psql -v ON_ERROR_STOP=1 \
        --username "$POSTGRES_USER" \
        --dbname "$POSTGRES_DB" \
        -c "\copy raw_sales FROM '$f' WITH (FORMAT csv, HEADER true, NULL '')"
done

psql -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    -c "SELECT COUNT(*) AS raw_sales_rows FROM raw_sales;"

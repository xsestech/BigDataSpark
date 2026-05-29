.PHONY: help initdb clean_initdb up down logs ps download_jars install_valkey_deps \
        submit_snowflake submit_reports submit_cassandra submit_neo4j submit_mongo submit_valkey \
        submit_all psql clickhouse cqlsh cypher mongosh redis

JARS_DIR     := jars
CH_JDBC      := $(JARS_DIR)/clickhouse-jdbc-0.9.5-all.jar
CH_SPARK     := $(JARS_DIR)/clickhouse-spark-runtime-3.5_2.12-0.10.0.jar
CASSANDRA_JAR:= $(JARS_DIR)/spark-cassandra-connector-assembly_2.12-3.5.1.jar
NEO4J_JAR    := $(JARS_DIR)/neo4j-connector-apache-spark_2.12-5.3.4_for_spark_3.jar
MONGO_JAR    := $(JARS_DIR)/mongo-spark-connector_2.12-10.4.0.jar

PG_PKG       := org.postgresql:postgresql:42.7.4
MONGO_PKG    := org.mongodb.spark:mongo-spark-connector_2.12:10.4.0

SPARK_MEMORY_CAP := 4g
SPARK_SUBMIT := docker compose exec -T spark /opt/spark/bin/spark-submit \
                --master local[*] \
				--driver-memory $(SPARK_MEMORY_CAP) \
                --conf spark.jars.ivy=/root/.ivy2

help:
	@echo "Targets:"
	@echo "  up                build initdb, download jars, start containers"
	@echo "  down              stop containers and clean generated state"
	@echo "  download_jars     fetch all Spark connector JARs into ./jars (idempotent)"
	@echo "  submit_snowflake  PostgreSQL raw -> snowflake schema in PostgreSQL"
	@echo "  submit_reports    snowflake -> 6 reports in ClickHouse"
	@echo "  submit_cassandra  snowflake -> 6 reports in Cassandra"
	@echo "  submit_neo4j      snowflake -> 6 reports in Neo4j"
	@echo "  submit_mongo      snowflake -> 6 reports in MongoDB"
	@echo "  submit_valkey     snowflake -> 6 reports in Valkey"
	@echo "  submit_all        run snowflake + all 5 report jobs"
	@echo "  psql / clickhouse / cqlsh / cypher / mongosh / redis  open DB shells"

initdb:
	@mkdir -p initdb-postgres initdb-clickhouse
	@cp sql/postgres/*.sql initdb-postgres/
	@cp scripts/postgres/*.sh initdb-postgres/
	@chmod +x initdb-postgres/*.sh
	@cp sql/clickhouse/*.sql initdb-clickhouse/

clean_initdb:
	@rm -rf initdb-postgres initdb-clickhouse

$(CH_JDBC):
	@mkdir -p $(JARS_DIR)
	@echo "Downloading $(@F)"
	@curl -fsSL -o $@ https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.9.5/clickhouse-jdbc-0.9.5-all.jar

$(CH_SPARK):
	@mkdir -p $(JARS_DIR)
	@echo "Downloading $(@F)"
	@curl -fsSL -o $@ https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-3.5_2.12/0.10.0/clickhouse-spark-runtime-3.5_2.12-0.10.0.jar

$(CASSANDRA_JAR):
	@mkdir -p $(JARS_DIR)
	@echo "Downloading $(@F)"
	@curl -fsSL -o $@ https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.5.1/spark-cassandra-connector-assembly_2.12-3.5.1.jar

$(NEO4J_JAR):
	@mkdir -p $(JARS_DIR)
	@echo "Downloading $(@F)"
	@curl -fsSL -o $@ https://repo1.maven.org/maven2/org/neo4j/neo4j-connector-apache-spark_2.12/5.3.4_for_spark_3/neo4j-connector-apache-spark_2.12-5.3.4_for_spark_3.jar

$(MONGO_JAR):
	@mkdir -p $(JARS_DIR)
	@echo "Downloading $(@F)"
	@curl -fsSL -o $@ https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.4.0/mongo-spark-connector_2.12-10.4.0.jar

download_jars: $(CH_JDBC) $(CH_SPARK) $(CASSANDRA_JAR) $(NEO4J_JAR) $(MONGO_JAR)
	@echo "JARs ready in $(JARS_DIR)"

install_valkey_deps:
	@docker compose exec -T spark pip install --quiet --no-input redis

up: initdb download_jars
	@docker compose up -d --force-recreate

down: clean_initdb
	@docker compose down -v

logs:
	@docker compose logs -f --tail=100

ps:
	@docker compose ps

submit_snowflake:
	$(SPARK_SUBMIT) --packages $(PG_PKG) /opt/jobs/etl_snowflake.py

submit_reports:
	$(SPARK_SUBMIT) --packages $(PG_PKG) \
		--jars /opt/extra-jars/clickhouse-jdbc-0.9.5-all.jar,/opt/extra-jars/clickhouse-spark-runtime-3.5_2.12-0.10.0.jar \
		/opt/jobs/etl_reports.py

submit_cassandra:
	$(SPARK_SUBMIT) --packages $(PG_PKG) \
		--jars /opt/extra-jars/spark-cassandra-connector-assembly_2.12-3.5.1.jar \
		/opt/jobs/etl_cassandra_reports.py

submit_neo4j:
	$(SPARK_SUBMIT) --packages $(PG_PKG) \
		--jars /opt/extra-jars/neo4j-connector-apache-spark_2.12-5.3.4_for_spark_3.jar \
		/opt/jobs/etl_neo4j_reports.py

submit_mongo:
	$(SPARK_SUBMIT) --packages $(PG_PKG),$(MONGO_PKG) /opt/jobs/etl_mongo_reports.py

submit_valkey: install_valkey_deps
	$(SPARK_SUBMIT) --packages $(PG_PKG) /opt/jobs/etl_valkey_reports.py

submit_all: submit_snowflake submit_reports submit_cassandra submit_neo4j submit_mongo submit_valkey

psql:
	@docker compose exec postgres psql -U admin -d sales

clickhouse:
	@docker compose exec clickhouse clickhouse-client --user admin --password admin --database reports

cqlsh:
	@docker compose exec cassandra cqlsh -u cassandra -p cassandra -k reports

cypher:
	@docker compose exec neo4j cypher-shell -u neo4j -p testtest1

mongosh:
	@docker compose exec mongodb mongosh -u admin -p admin --authenticationDatabase admin reports

redis:
	@docker compose exec valkey valkey-cli

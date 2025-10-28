Data Processing Challenge, Shivaathmajan P 23BIT101
Use Apache Kafka, Spark, and Debezium for real-time streaming, CDC, and preprocessing.

How to run this program:
# 0) Python env
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt

# 1) Start infra
docker compose up -d

# 2) Create topic (copy the exact container name from `docker ps` if different)
docker exec -it dp_challenge_spark_kafka-kafka-1 kafka-topics --create --topic transactions --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1

# 3) Register CDC connector
curl -X POST -H "Content-Type: application/json" -d "@connectors/pg_inventory_connector.json" http://localhost:8083/connectors

# 4) Send sample events
python src/streaming/producer.py

# 5) Run Spark streaming (include Kafka connector package)
python -m pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src/streaming/spark_job.py

# 6) (Optional) Run CDC consumer
python -m pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src/cdc/cdc_spark_job.py

# 7) (Optional) In-memory demo
python src/inmemory/inmemory_demo.py


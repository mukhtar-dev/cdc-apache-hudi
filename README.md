# 🧪 Change Data Capture with Apache Hudi, Debezium, Kafka & Spark

This project demonstrates a complete Change Data Capture (CDC) pipeline using PostgreSQL, Debezium, Kafka, Apache Hudi, and Apache Spark. It captures real-time changes from a PostgreSQL database, streams them via Kafka, writes to a Hudi data lake, and incrementally updates an aggregated table.

---

## 🔧 Technologies Used

- **PostgreSQL** – Source database with logical replication enabled.
- **Debezium** – Captures database changes as events and sends them to Kafka.
- **Kafka & Zookeeper** – Message streaming platform.
- **Schema Registry** – Manages Avro schemas used in CDC events.
- **Apache Hudi** – Incrementally stores CDC data into a data lake.
- **Apache Spark** – Reads CDC data, transforms, and updates Hudi tables.

---

## 🏗️ Architecture Overview
```text

PostgreSQL (retail_transactions table)
│
▼
Debezium (CDC)
│
▼
Kafka (sampleTopic)
│
▼
Spark Streaming (kafka_to_hudi_stream.py)
│
▼
Hudi Base Table (/hudi_data/retails_table)
│
▼
Batch Spark Job (update_hudi_agg_table.py)
│
▼
Hudi Aggregated Table (/hudi_data/aggregated_orders)
```

---

## 📁 Project Structure
```text
.
├── docker-compose.yml
├── connector.json
├── scripts/
│   └── init.sh               # PostgreSQL initialization
├── jars_dir/                 # Optional Spark JARs
└── spark-apps/
    ├── kafka_to_hudi_stream.py
    ├── update_hudi_agg_table.py
    ├── query_hudi_base_table.py
    └── query_hudi_agg_table.py
```

---

## 🚀 Getting Started
### 🖥️ Prerequisites
- Docker Desktop with WSL 2 integration enabled (on Windows)
- Python 3 and Spark installed inside the spark-master container (done during setup)
- Basic familiarity with docker, psql, and spark-submit

## 🛠️ Setup Instructions
### 1. Clone the Repository

```bash
git clone https://github.com/your-username/cdc-apache-hudi.git
cd cdc-apache-hudi
```

### 2. Start Docker Environment

```bash
docker-compose up --build
```

### 3. Prepare PostgreSQL

```bash
docker exec -it hudidb bash
psql -U postgres -f /docker-entrypoint-initdb.d/init.sh
```
Verify the data:
```bash
psql -U postgres -d dev -c "SELECT * FROM V1.retail_transactions;"
```

### 4. Register Debezium Connector

```bash
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @connector.json
```

### 5. Prepare Spark Environment 
Create Hudi Data Lake Directory
```bash
docker exec spark-master mkdir -p /hudi_data
```
From spark-master container Install Python Dependencies in Spark
```bash
docker exec -it spark-master bash
pip install requests
```

# 📡 Running the Applications

## 🔁 Stream CDC to Hudi Base Table
```bash
spark-submit \
  --packages org.apache.hudi:hudi-spark3.1-bundle_2.12:0.15.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,\
org.apache.spark:spark-avro_2.12:3.1.2 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar \
  /opt/spark-apps/kafka_to_hudi_stream.py
```

## 📁 Verify Hudi Base Table
```bash
spark-submit \
  --packages org.apache.hudi:hudi-spark3.1-bundle_2.12:0.15.0 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  /opt/spark-apps/query_hudi_base_table.py
```

## 📊 Run Aggregation Job
```bash
spark-submit \
  --packages org.apache.hudi:hudi-spark3.1-bundle_2.12:0.15.0 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  /opt/spark-apps/update_hudi_agg_table.py
```

## 📋 Verify Aggregated Table
```bash
spark-submit \
  --packages org.apache.hudi:hudi-spark3.1-bundle_2.12:0.15.0 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
  /opt/spark-apps/query_hudi_agg_table.py
```

# ✏️ Simulate New Changes

Go back to PostgreSQL container:
```bash
docker exec -it hudidb bash
psql -U postgres -d dev
```
Insert new rows:
```bash
\connect dev;
SET search_path TO V1;

INSERT INTO retail_transactions VALUES (5, '2019-03-11', 1, 'CHICAGO', 'IL', 9, 146.25);
INSERT INTO retail_transactions VALUES (25, '2022-03-11', 1, 'SPRINGFIELD', 'IL', 33, 146.25);
```
Then repeat the **aggregation job** and **query** to verify updates.
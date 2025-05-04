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


---

## 🚀 How to Run

### 1. Start Docker Environment

```bash
cd C:\DataProjects\cdc-apache-hudi
docker-compose up --build


### 2. Prepare PostgreSQL

```bash
docker exec -it hudidb bash
psql -U postgres -f /docker-entrypoint-initdb.d/init.sh
psql -U postgres -d dev -c "SELECT * FROM V1.retail_transactions;"


### 3. Register Debezium Connector

```bash
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @connector.json

### 4. Create Hudi Data Lake Directory
```bash

### 4. Create Hudi Data Lake Directory
```bash
### 4. Create Hudi Data Lake Directory
```bash
### 4. Create Hudi Data Lake Directory
```bash
### 4. Create Hudi Data Lake Directory
```bash

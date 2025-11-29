
### **Data Engineering ETL Task**

A complete **streaming + batch data engineering pipeline** built for Azki, supporting:

* **Real-time ingestion** of user events and user data
* **CDC replication** from MySQL using Debezium
* **Denormalized purchase modeling** across multiple order sources
* **Real-time & batch aggregation** using ClickHouse
* **Data quality, monitoring, and backfill workflows**
* Fully containerized infrastructure via **Docker Compose**

---

## **🧩 Architecture Components**

| Component                    | Purpose                         |
| ---------------------------- | ------------------------------- |
| **Kafka / Redpanda**         | Event streaming backbone        |
| **Kafka Connect + Debezium** | CDC from MySQL → Kafka          |
| **Schema Registry**          | Schema evolution & validation   |
| **MySQL**                    | Operational user data source    |
| **ClickHouse**               | Analytical data warehouse       |
| **Spark**                    | Batch ETL & backfill processing |
| **Docker Compose**           | Local infra orchestration       |

---

## **🚀 Getting Started**

### **1. Start the Infrastructure**

```bash
docker-compose up -d
```

This launches:

* MySQL
* Redpanda / Kafka
* Schema Registry
* Kafka Connect
* ClickHouse

---

### **2. Install Python Dependencies**

```bash
pip install -r requirements.txt
```

---

### **3. Start the Kafka Producer**

Replay `user_events.csv` into Kafka:

```bash
python etls/produce_user_events.py
```

---

### **4. Create ClickHouse Tables**

Run ClickHouse SQL scripts:

```bash
clickhouse-client --queries-file clickhouse/sql/tables.sql
```

(Or import each file individually.)

---

### **5. Run Batch Aggregation**

Daily batch aggregation or backfill job:

```bash
python etl/batch_processing/batch_user_events_agg.py
```

---

## **📊 UI Access (Redpanda Console)**

You can inspect Kafka topics here:

👉 **[http://localhost:8080/topics](http://localhost:8080/topics)**



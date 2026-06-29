# Address Intelligence Pipeline (V1 POC)

A containerized big-data pipeline that classifies delivery addresses as **Corporate** or **Residential** using a rule-based keyword engine, built with **PySpark** and **PostgreSQL**, fully orchestrated via **Docker Compose**.

This is the V1 baseline of a larger address-intelligence system that will eventually scale to Kafka streaming, geocoding enrichment, and ML-based classification.

---

## Problem Statement

In last-mile delivery operations, knowing whether a recipient's address is a **corporate/business** location or a **residential** one helps optimize:
- Delivery time-slot scheduling (office hours vs evening)
- Routing and SLA prediction
- Fraud/anomaly detection
- Operational reporting

This project classifies raw, unlabeled address records at ingestion time and persists the classification — along with explainability metadata — into a relational store for downstream consumption.

---

## Architecture (V1)

```
┌─────────────────┐      ┌──────────────────────┐      ┌──────────────────┐
│   addresses.csv  │ ───▶ │   PySpark Container   │ ───▶ │   PostgreSQL DB   │
│ (raw, unlabeled)  │      │  (classify.py job)    │      │  (addresses table)│
└─────────────────┘      └──────────────────────┘      └──────────────────┘
```

| Component | Role |
|---|---|
| **Docker Compose** | Orchestrates both containers, manages networking & volumes |
| **PySpark container** | Reads raw CSV, runs keyword-based classification logic, writes results via JDBC |
| **PostgreSQL container** | Stores raw input fields + predicted `address_type`, `matched_keywords`, `confidence` |

This mirrors the same ingestion → processing → storage pattern used in production big-data pipelines (Kafka/HDFS → Spark → Hive/RDBMS), scaled down to a single-node Docker setup for rapid prototyping.

---

## How Classification Works

The V1 classifier is a **rule-based keyword matcher** — no ML model yet. It scans each address string for two keyword sets:

**Corporate signals:** `pvt ltd`, `llp`, `tower`, `floor`, `park`, `solutions`, `technologies`, `plot no`, `it park`, `suite`, `building`, `corp`, `industries`, `business`, `enterprises`, `estate`, `midc`

**Residential signals:** `house no`, `h.no`, `flat`, `society`, `nagar`, `apartment`, `colony`, `residency`, `row house`

### Decision logic
- Only **corporate** keywords matched → `Corporate`
- Only **residential** keywords matched → `Residential`
- **Both** matched → `Unknown` (ambiguous — flagged for manual review)
- **Neither** matched → `Unknown`

### Explainability fields
Every prediction is stored with:
- `matched_keywords` — exactly which keyword(s) triggered the decision
- `confidence` — `High` (2+ keyword hits), `Low` (1 hit), or `None` (0 hits)

This makes every classification auditable — critical for debugging misclassifications and for building a future labeled training set from the `Unknown`/`Low` confidence rows.

---

## Project Structure

```
address-intel-pipeline/
├── docker-compose.yml
├── Dockerfile.pyspark
├── requirements.txt
├── data/
│   └── addresses.csv        # raw, unlabeled input
├── sql/
│   └── init.sql             # Postgres schema, auto-applied on first startup
└── src/
    └── classify.py          # PySpark classification job
```

---

## Tech Stack

- **PySpark 3.5.1** — distributed data processing
- **PostgreSQL 16** — relational storage
- **Docker / Docker Compose** — containerized environment
- **JDBC (postgresql-42.7.3.jar)** — Spark-to-Postgres connectivity
- **Python 3.11** — base runtime

---

## Setup & Run

### Prerequisites
- Docker & Docker Compose installed
- Ports `5432` (Postgres) and `4040` (Spark UI) free on host

### 1. Clone and build
```bash
git clone https://github.com/iamma00/address-intel-pipeline.git
cd address-intel-pipeline
docker-compose up -d --build
```

### 2. Run the classification job
```bash
docker exec -it address_poc_spark bash
spark-submit --jars /opt/spark-jars/postgresql-42.7.3.jar /app/src/classify.py
```

> **Note:** The `--jars` flag is required at submit time for the driver JVM to correctly register the PostgreSQL JDBC driver class — setting it only via `spark.jars` in code is not sufficient.

### 3. Verify results in Postgres
```bash
docker exec -it address_poc_db psql -U poc_user -d address_poc
```
```sql
SELECT name, address_type, matched_keywords, confidence FROM addresses;
SELECT address_type, COUNT(*) FROM addresses GROUP BY address_type;
```

---

## Sample Output

| name | address | address_type | matched_keywords | confidence |
|---|---|---|---|---|
| Acme Corp | Plot No 12 IT Park Tower B Floor 4 | Corporate | tower, floor, park, plot no, it park | High |
| Ravi Kumar | House No 45 Shanti Nagar Society | Residential | house no, society, nagar | High |
| NextGen Software Pvt Ltd | 3rd Floor Park Plaza Viman Nagar | Unknown | floor, park, nagar | High |

```
+------------+-----+
|address_type|count|
+------------+-----+
| Residential|   14|
|     Unknown|    3|
|   Corporate|   13|
+------------+-----+
```

---

## Known Limitations (V1)

- Pure keyword matching — no semantic understanding of address text
- No geocoding/POI validation yet (planned for V2)
- `Unknown` bucket needs manual review — not yet fed back into a learning loop
- No handling for multi-language or heavily abbreviated addresses
- Single-node batch processing — no streaming ingestion yet

---

## Roadmap

- [ ] **V2 — Geocoding enrichment**: integrate Nominatim/OSM (self-hosted Docker container) to validate keyword predictions against real POI data
- [ ] **V3 — ML classifier**: use manually reviewed `Unknown`/`Low confidence` rows as training data for a TF-IDF + Logistic Regression or Gradient Boosted model
- [ ] **V4 — Streaming ingestion**: replace batch CSV read with Kafka producer/consumer + Spark Structured Streaming
- [ ] **V5 — Production-grade orchestration**: Airflow DAGs for scheduled retraining/scoring, Great Expectations for data quality validation, Apache Atlas for lineage tracking
- [ ] Entity-resolution layer to catch shared/co-working office addresses that look residential

---

## Author

**Mahmood Ansari**
Cloudera & Hadoop Administrator | Big Data Infrastructure
GitHub: [@iamma00](https://github.com/iamma00) · LinkedIn: [iamma00](https://linkedin.com/in/iamma00)

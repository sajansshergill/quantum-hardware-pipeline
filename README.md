# Quantum Hardware Performance Intelligence Pipeline (in-progress)

> End-to-end data engineering pipeline for quantum device telemetry ingestion, calibration drift detection, and hardware reliability analytics — built to mirror IBM Quantum's production observability stack.

---

## Overview

Real quantum computers generate continuous streams of hardware telemetry: qubit coherence times, gate error rates, readout fidelity, calibration logs, job execution records, and system health events. This pipeline ingests, transforms, and surfaces those signals into a trusted analytical layer that engineering and product teams can act on.

This project simulates that environment end-to-end — synthetic QPU telemetry flows through Kafka, gets orchestrated by Airflow, lands in a medallion lakehouse (DuckDB + dbt), and is surfaced via a Streamlit dashboard with calibration drift alerting.

**Targeted role:** IBM Quantum — Data Engineer (Data Integration)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Synthetic Data Generator                   │
│  QPU Telemetry · Calibration Logs · Job Execution · Health  │
└───────────────────────┬─────────────────────────────────────┘
                        │ Python producers
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                      Apache Kafka                            │
│  qpu.telemetry · qpu.calibration · qpu.jobs · qpu.health   │
└───────────────────────┬─────────────────────────────────────┘
                        │ Kafka consumers
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                    Apache Airflow                            │
│  DAG: ingest → validate → transform → drift_detect → alert  │
└───────────────────────┬─────────────────────────────────────┘
                        │ writes
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              DuckDB Lakehouse  +  dbt transforms             │
│  bronze (raw) → silver (validated) → gold (reliability mart) │
└───────────────────────┬─────────────────────────────────────┘
                        │ reads
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                  Streamlit Dashboard                         │
│  Qubit fidelity · Gate errors · Drift alerts · Job SLA      │
└─────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Streaming | Apache Kafka (Confluent) |
| Orchestration | Apache Airflow |
| Storage & Query | DuckDB + Parquet |
| Transformation | dbt (DuckDB adapter) |
| Data Generation | Python (Faker, NumPy) |
| Dashboard | Streamlit |
| Testing | pytest (unit + integration) |
| CI/CD | GitHub Actions |
| Containerization | Docker + Docker Compose |

---

## Project Structure

```
quantum-hardware-pipeline/
│
├── README.md
├── docker-compose.yml              # Kafka + Zookeeper + Airflow
├── requirements.txt
├── .env.example
│
├── generator/                      # Synthetic QPU data
│   ├── qpu_devices.py              # Device registry (ibm_fez, ibm_torino, ...)
│   ├── telemetry_producer.py       # Kafka producer: T1/T2, gate errors, readout fidelity
│   ├── calibration_producer.py     # Kafka producer: calibration run logs
│   ├── jobs_producer.py            # Kafka producer: job execution records
│   └── health_producer.py          # Kafka producer: system health events
│
├── ingestion/                      # Kafka consumers → bronze layer
│   ├── consumers/
│   │   ├── telemetry_consumer.py
│   │   ├── calibration_consumer.py
│   │   ├── jobs_consumer.py
│   │   └── health_consumer.py
│   └── schema/
│       ├── telemetry_schema.json
│       ├── calibration_schema.json
│       ├── jobs_schema.json
│       └── health_schema.json
│
├── dags/                           # Airflow DAGs
│   ├── ingest_dag.py               # Pull from Kafka → bronze Parquet
│   ├── transform_dag.py            # bronze → silver → gold via dbt
│   ├── drift_detection_dag.py      # Z-score calibration drift alerting
│   └── sla_monitor_dag.py          # Job execution SLA compliance
│
├── dbt/                            # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── bronze/
│   │   │   ├── raw_telemetry.sql
│   │   │   ├── raw_calibration.sql
│   │   │   ├── raw_jobs.sql
│   │   │   └── raw_health.sql
│   │   ├── silver/
│   │   │   ├── stg_telemetry.sql   # validate + dedupe + type cast
│   │   │   ├── stg_calibration.sql
│   │   │   ├── stg_jobs.sql
│   │   │   └── stg_health.sql
│   │   └── gold/
│   │       ├── dim_device.sql          # device registry dimension
│   │       ├── dim_qubit.sql           # qubit-level dimension
│   │       ├── fct_telemetry.sql       # fact: per-qubit per-run metrics
│   │       └── device_reliability_mart.sql  # aggregated reliability KPIs
│   └── tests/
│       ├── not_null_device_id.sql
│       ├── valid_fidelity_range.sql
│       └── no_duplicate_calibration_runs.sql
│
├── detection/
│   └── drift_detector.py           # Z-score + rolling window on gate error rates
│
├── dashboard/
│   └── app.py                      # Streamlit: fidelity trends, drift alerts, job SLA
│
└── tests/
    ├── test_generator.py
    ├── test_consumers.py
    ├── test_drift_detector.py
    └── test_dbt_models.py
```

---

## Data Model

### Synthetic QPU Devices

The generator simulates a small fleet of named quantum devices, each with a configurable number of qubits and baseline performance characteristics.

| Field | Description | Example |
|---|---|---|
| `device_id` | Unique device identifier | `ibm_fez` |
| `num_qubits` | Qubit count | `127` |
| `backend_version` | Firmware version | `1.3.2` |
| `region` | Cloud region | `us-east` |

### Telemetry Schema (`qpu.telemetry`)

| Field | Type | Description |
|---|---|---|
| `event_id` | UUID | Unique telemetry event |
| `device_id` | STRING | Source QPU device |
| `qubit_id` | INT | Qubit index |
| `T1_us` | FLOAT | Relaxation time (microseconds) |
| `T2_us` | FLOAT | Dephasing time (microseconds) |
| `gate_error_1q` | FLOAT | Single-qubit gate error rate |
| `gate_error_2q` | FLOAT | Two-qubit gate error rate |
| `readout_fidelity` | FLOAT | Measurement fidelity (0–1) |
| `timestamp` | TIMESTAMP | Event time |

### Gold Mart: `device_reliability_mart`

Aggregated per device per day — the primary analytical asset for the dashboard.

| Field | Description |
|---|---|
| `device_id` | QPU device |
| `date` | Measurement date |
| `avg_T1_us` | Mean relaxation time across qubits |
| `avg_T2_us` | Mean dephasing time |
| `p99_gate_error_1q` | 99th percentile single-qubit gate error |
| `p99_gate_error_2q` | 99th percentile two-qubit gate error |
| `min_readout_fidelity` | Worst-case readout fidelity |
| `calibration_count` | Calibration runs completed |
| `drift_flag` | Boolean: z-score breach on any qubit |
| `job_sla_pct` | % of jobs completing within SLA window |

---

## Key Features

### Calibration Drift Detection

The `drift_detector.py` module watches rolling gate error rates per qubit. When a qubit's 1Q or 2Q gate error deviates more than **2.5 standard deviations** from its 30-run rolling mean, a `drift_flag` is raised in the gold mart and surfaced as an alert in the dashboard. This mirrors how IBM Quantum's SRE team monitors device health between scheduled calibration cycles.

### Medallion Lakehouse

| Layer | What it contains |
|---|---|
| **Bronze** | Raw Kafka payloads, exactly as consumed — no transformation |
| **Silver** | Validated, deduplicated, type-cast records with data quality flags |
| **Gold** | `device_reliability_mart` + dimensional models for analytics |

### Airflow DAG Graph

```
ingest_dag (every 5 min)
  └── consume_telemetry
  └── consume_calibration
  └── consume_jobs
  └── consume_health

transform_dag (every 15 min, depends on ingest_dag)
  └── dbt run --select bronze
  └── dbt run --select silver
  └── dbt run --select gold
  └── dbt test

drift_detection_dag (every 30 min)
  └── compute_rolling_stats
  └── flag_drift_qubits
  └── write_alerts_to_gold

sla_monitor_dag (daily)
  └── compute_job_sla_compliance
  └── update_reliability_mart
```

---

## Dashboard Panels

| Panel | Description |
|---|---|
| **Qubit Fidelity Heatmap** | Per-device, per-qubit readout fidelity over time |
| **Gate Error Trends** | 1Q and 2Q gate error rate time series with drift alert overlay |
| **Calibration Drift Alerts** | Table of flagged qubits with z-score, device, and timestamp |
| **Job SLA Compliance** | Daily % of jobs completing within the SLA window |

---

## Setup & Run

### Prerequisites

- Docker + Docker Compose
- Python 3.11+
- Node.js (optional, for local dbt docs)

### Quickstart

```bash
# 1. Clone and configure
git clone https://github.com/sajansshergill/quantum-hardware-pipeline.git
cd quantum-hardware-pipeline
cp .env.example .env

# 2. Start Kafka + Airflow
docker-compose up -d

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Start synthetic data producers
python generator/telemetry_producer.py &
python generator/calibration_producer.py &
python generator/jobs_producer.py &
python generator/health_producer.py &

# 5. Trigger the pipeline manually (or let Airflow schedule it)
airflow dags trigger ingest_dag
airflow dags trigger transform_dag

# 6. Run dbt models
cd dbt && dbt run && dbt test

# 7. Launch dashboard
streamlit run dashboard/app.py
```

### Run Tests

```bash
pytest tests/ -v --tb=short
```

---

## IBM Quantum JD Alignment

| JD Requirement | Implementation |
|---|---|
| Design & build scalable ETL/ELT pipelines | Airflow DAGs: ingest → validate → transform → alert |
| Lakehouse architecture | DuckDB + dbt medallion (bronze/silver/gold) |
| PostgreSQL / Presto-style SQL | DuckDB SQL; gold mart queries translate 1:1 to Presto |
| Apache Kafka / IBM Event Streams | Kafka producers + consumers for all 4 QPU data streams |
| Streaming architecture (topics, partitions, consumer groups) | Partitioned by `device_id`; separate consumer groups per layer |
| Apache Airflow orchestration | Dependency-managed DAGs with retries, backfill, SLA hooks |
| Hardware telemetry & operational datasets | Simulated T1/T2, gate errors, readout fidelity, job records |
| Data quality, accuracy, timeliness | dbt tests (not null, range, dedup) + Great Expectations layer |
| Python (pandas, NumPy) | Generator, drift detector, consumer transforms |
| Git, code reviews, engineering best practices | GitHub Actions CI: pytest + dbt test on every PR |

---

## Author

**Sajan S. Shergill**
M.S. Data Science, Pace University (May 2026)
[linkedin.com/in/sajanshergill](https://linkedin.com/in/sajanshergill) · [sajansshergill.github.io](https://sajansshergill.github.io)

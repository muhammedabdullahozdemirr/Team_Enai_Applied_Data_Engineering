# YZV 322E — E-Commerce Real-Time Pipeline

> **YZV 322E — Applied Data Engineering · Final Project**
>
> Team Enai · Istanbul Technical University

End-to-end, fully containerized streaming data pipeline for a synthetic Turkish e-commerce platform. The whole stack — from Kafka to Airflow — comes up with a single `docker compose up --build`.

---

## Project Summary

We simulate a Turkish e-commerce site producing realistic user behavior events: `page_view → product_click → add_to_cart → checkout_start → purchase`. A Python producer pushes ~30,000 events to Kafka with a configurable rate (default 100 events/sec) and intentional data-quality issues injected at 2.5%.

NiFi consumes from Kafka, validates each event, routes valid ones to Elasticsearch (real-time analytics) and corrupt ones to a Postgres dead-letter queue (DLQ) as `JSONB`. Kibana provides four dashboards over the ES index. Airflow orchestrates three DAGs that periodically aggregate metrics, check data quality ratios, and inspect the DLQ.

The architecture demonstrates **streaming ingestion**, **per-event routing with failure isolation**, **multi-store fanout** (Elasticsearch + Postgres), and **batch orchestration on top of streaming output**.

---

## Tech Stack

We use 5 course tools + Kafka.

| Tool | Role | Justification |
|---|---|---|
| **Apache Kafka** | Event broker | Decouples producer from consumers, durable buffer for backpressure |
| **Apache NiFi** | Stream processor | Visual pipeline, built-in failure routing, low-code DLQ pattern |
| **Elasticsearch** | Document store | Real-time search & aggregation for event analytics |
| **Kibana** | Dashboarding | Native ES integration, no extra config for visualizations |
| **PostgreSQL** | DLQ + metrics + lookup | `JSONB` for original DLQ events, time-series for pipeline metrics, products catalog |
| **pgAdmin** | DB administration | Visual query/inspection during development & demo |
| **Apache Airflow** | Batch orchestration | DAG-based scheduling for aggregation, data quality, reconcile |

---

## Quick Start

Prerequisites: Docker Desktop (≥4.x), 16 GB RAM recommended, ~20 GB free disk.

```bash
# 1. Clone and enter
git clone https://github.com/muhammedabdullahozdemirr/Team_Enai_Applied_Data_Engineering.git
cd Team_Enai_Applied_Data_Engineering

# 2. Configure environment
cp .env.example .env

# 3. Download NiFi Postgres JDBC driver (one-time)
cd nifi/drivers && ./download.sh && cd ../..

# 4. Bring up the entire stack
docker compose up --build
```

The first run takes ~5–10 minutes (image pulls). Subsequent starts: ~60 seconds.

After startup:
1. **NiFi flow:** Import `nifi/exported/dlq-flow.json` via NiFi UI (Operate panel → Upload Process Group)
2. **Kibana dashboards:** `kibana/dashboard.ndjson` is auto-loaded; otherwise import via Stack Management → Saved Objects
3. **Airflow DAGs:** Visible automatically at `localhost:8081`; toggle each on and trigger

---

## Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8090 | — |
| pgAdmin | http://localhost:5050 | from `.env` (`PGADMIN_EMAIL`/`PGADMIN_PASSWORD`) |
| NiFi | http://localhost:8080/nifi | `admin` / `yzv322e_admin_pass` |
| Elasticsearch | http://localhost:9200 | — |
| Kibana | http://localhost:5601 | — |
| Airflow | http://localhost:8081 | `admin` / `yzv322e_admin_pass` |

---

## Verifying the Pipeline

After producer finishes (~5 minutes), run:

```bash
./scripts/verify-pipeline.sh
```

Expected output:
```
Elasticsearch ecommerce-events:    ~29,070 docs
Postgres dead_letter_events:       ~930 rows (null_user_id ~857, negative_price ~73)
Total processed:                   ~30,000 events
```

Quick SQL probe:
```bash
docker compose exec postgres psql -U ecom_user -d ecommerce \
  -c "SELECT error_type, COUNT(*) FROM ecommerce.dead_letter_events GROUP BY error_type;"
```

---

## Airflow DAGs

| DAG | Purpose | Output |
|---|---|---|
| `hourly_aggregation` | Pulls last hour of events from Elasticsearch, aggregates by city / device / event type, computes purchase revenue | 21 metrics → `pipeline_metrics` |
| `data_quality_check` | Computes DLQ-to-total ratio, alerts if above 5% threshold | 7 metrics → `pipeline_metrics` |
| `dlq_reconcile` | Inspects DLQ, classifies events as recoverable / reviewable / archivable (skeleton) | 5 metrics → `pipeline_metrics` |

All DAGs write structured key-value metrics to `ecommerce.pipeline_metrics`, enabling downstream BI consumption.

---

## Repository Structure

```
.
├── dags/                       # Airflow DAGs (3 production + skeleton)
├── data/sample/                # Sample event data
├── docker/pgadmin/             # pgAdmin server config
├── docs/
│   ├── data-schema.md          # Event schema spec
│   └── PIPELINE-SETUP.md       # NiFi flow setup walkthrough
├── kibana/dashboard.ndjson     # Saved Kibana dashboards & visualizations
├── nifi/
│   ├── drivers/                # Postgres JDBC driver
│   └── exported/NiFi_Flow.json  # NiFi flow definition (importable)
├── scripts/
│   ├── verify-pipeline.sh      # Pipeline health check
│   └── benchmark.sh            # Throughput / latency benchmark
├── sql/init/
│   ├── 01_schema.sql           # ecommerce.products, dead_letter_events, pipeline_metrics
│   └── 02_seed_products.sql    # 500 product rows
├── src/producer/               # Python event generator (Dockerized)
├── tests/                      # pytest suite (data quality, schema)
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## Performance

We benchmarked the pipeline at five producer rates with 10,000 events per run:

| Target Rate | Producer Actual | Pipeline Throughput | Drain Lag | ES Indexed | DLQ | Loss |
|--:|--:|--:|--:|--:|--:|--:|
| 50 events/s | 49.50/s | 47.39/s | 9s | 9,710 | 290 | 0 |
| 100 events/s | 98.04/s | 89.29/s | 10s | 9,698 | 302 | 0 |
| 250 events/s | 238.10/s | 196.10/s | 9s | 9,701 | 300 | 0 |
| 500 events/s | 434.78/s | 312.50/s | 9s | 9,709 | 291 | 0 |
| 1000 events/s | 833.33/s | 476.19/s | 9s | 9,653 | 347 | 0 |

Full results in `bench/results.csv`. Reproduce with `./scripts/benchmark.sh`.

---

## Known Limitations

- **Single-broker Kafka** — no replication, suitable for academic demo only
- **Airflow uses SQLite metadata + SequentialExecutor** — single-task-at-a-time; production deployments should use Postgres backend with LocalExecutor or CeleryExecutor
- **No Postgres lookup enrichment in NiFi** — `product_brand`, `stock_level`, `is_featured` enrichment is left as future work; current pipeline keeps events as raw producer payload
- **`invalid_timestamp` events bypass DLQ** — RouteOnAttribute does not route them, future work
- **DAGs are not idempotent** — each manual run appends to `pipeline_metrics`; production would partition by date or upsert
- **Synthetic data** — generator approximates real e-commerce funnel ratios but lacks edge cases of production traffic

---

## Future Work

- Add NiFi `LookupRecord` for product enrichment from Postgres
- Switch Airflow to Postgres backend with LocalExecutor
- Add `invalid_timestamp` route in NiFi RouteOnAttribute
- Implement actual DLQ reconciliation logic in `dlq_reconcile`
- Multi-broker Kafka with proper replication
- Real-time alerting via Slack/email when DQ threshold exceeded

---

## Team

| Member | Student ID |
|---|---|---|
| Muhammed Abdullah Özdemir | 150220340 |
| Nurettin Macit | 150220329 |
| Muhammed Hasan Bilal Cebeci | 150220339 |
| Muhammed Furkan Şıhanoğlu | 150220301 |

---

## License

MIT License — see [LICENSE](LICENSE).
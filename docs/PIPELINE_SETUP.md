# Pipeline Setup Guide

This guide walks through the post-startup steps required to bring the pipeline to a fully operational state after `docker compose up --build`.

The Docker stack starts all infrastructure (Kafka, NiFi, Elasticsearch, Kibana, PostgreSQL, pgAdmin, Airflow) automatically, but a few one-time configuration steps are required: importing the NiFi flow, loading the Kibana dashboard, and triggering Airflow DAGs.

---

## Prerequisites

- `docker compose up --build` has completed successfully
- All services report `running` or `healthy` status:
  ```bash
  docker compose ps
  ```
- The Postgres JDBC driver has been downloaded into `nifi/drivers/`:
  ```bash
  cd nifi/drivers && ./download.sh && cd ../..
  ```

---

## Step 1: Import the NiFi Flow

The NiFi pipeline definition lives in `nifi/exported/NiFi_Flow.json`. After NiFi starts, this flow must be imported once.

### Steps

1. Open NiFi UI: <http://localhost:8080/nifi>
2. Login with credentials from `.env` (`NIFI_USERNAME` / `NIFI_PASSWORD`)
3. On the empty canvas, drag a **Process Group** icon from the top toolbar onto the canvas
4. In the dialog that appears, click **Browse** and select `nifi/exported/NiFi_Flow.json`
5. Click **Add**
6. Double-click the imported process group to enter it
7. Right-click the canvas → **Configure** → **Controller Services** tab
8. Enable any disabled controller services (DBCPConnectionPool, JsonTreeReader, etc.)
9. Right-click the canvas → **Start** to begin processing

### Verification

Within ~30 seconds of starting, the processors should show non-zero `In` and `Out` counters in the bottom-right of each processor. Events should begin flowing to Elasticsearch and the DLQ in Postgres.

---

## Step 2: Load Kibana Dashboards

Kibana saved objects (index patterns, visualizations, dashboard) live in `kibana/dashboard.ndjson`.

### Steps

1. Open Kibana: <http://localhost:5601>
2. Navigate to **Stack Management** → **Saved Objects**
3. Click **Import** (top-right)
4. Select `kibana/dashboard.ndjson`
5. Choose **Automatically overwrite all conflicts**
6. Click **Import**

### Verification

Navigate to **Dashboard** → open **E-Commerce Real-Time Analytics**. Four visualizations (Event Types, Devices, Top Cities, Categories) should populate as soon as events flow through NiFi to Elasticsearch.

---

## Step 3: Run the Producer

The producer publishes 30,000 synthetic events to Kafka at 100 events/sec by default.

### Manual run (recommended for first-time validation)

```bash
docker compose run --rm producer
```

This blocks the terminal for ~5 minutes while events are produced. Output ends with a summary of injected DQ violations.

### Background run

```bash
docker compose up -d producer
```

Use `docker compose logs -f producer` to follow progress.

---

## Step 4: Trigger Airflow DAGs

Airflow boots in standalone mode with three production DAGs. They are off by default and must be toggled on once.

### Steps

1. Open Airflow UI: <http://localhost:8081>
2. Login with credentials from `.env` (`AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD`)
3. In the **DAGs** list, toggle each on:
   - `hourly_aggregation`
   - `data_quality_check`
   - `dlq_reconcile`
4. To trigger any DAG manually, click the **▶ Trigger DAG** button next to it

### Verification

After a successful run, query Postgres for metric output:

```bash
docker compose exec postgres psql -U ecom_user -d ecommerce -c \
  "SELECT measured_at, COUNT(*) FROM ecommerce.pipeline_metrics GROUP BY measured_at ORDER BY measured_at DESC LIMIT 5;"
```

Each DAG run inserts metrics tagged with the current timestamp (21 metrics for `hourly_aggregation`, 7 for `data_quality_check`, 5 for `dlq_reconcile`).

---

## End-to-End Verification

Run the verification script to confirm the entire pipeline is healthy:

```bash
./scripts/verify-pipeline.sh
```

Expected output (after producer completes one full run):

```
Elasticsearch ecommerce-events:    ~29,070 docs
Postgres dead_letter_events:       ~930 rows
Total processed:                   ~30,000 events
DLQ ratio:                         ~3.1%
```

For a quick DLQ breakdown:

```bash
docker compose exec postgres psql -U ecom_user -d ecommerce -c \
  "SELECT error_type, COUNT(*) FROM ecommerce.dead_letter_events GROUP BY error_type ORDER BY COUNT(*) DESC;"
```

---

## Troubleshooting

### NiFi flow shows red bullets on processors

Most often a controller service is not enabled, or the JDBC driver path is wrong:
- Right-click canvas → **Configure** → **Controller Services**
- Enable `DBCPConnectionPool`, ensure `Database Connection URL` is `jdbc:postgresql://postgres:5432/ecommerce` and the JDBC driver path points to `/opt/nifi/nifi-current/drivers/postgresql-42.7.3.jar`

### Kibana shows "No matching indices found"

The Elasticsearch index `ecommerce-events` does not exist yet. Run the producer at least once and ensure NiFi is started (events must reach Elasticsearch first).

### Airflow DAG fails with `psycopg2.OperationalError`

The `airflow` container needs `psycopg2-binary` installed. This is handled by `_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yml`. If the container started before this took effect, restart it:

```bash
docker compose restart airflow
```

### `docker compose down -v` does not free disk space

Volumes are removed but Docker images remain cached. To fully reclaim space:

```bash
docker compose down -v
docker system prune -af --volumes
```

Note: this removes **all** unused Docker resources system-wide.

---

## Architecture Recap

```
Producer → Kafka → NiFi → ┬→ Elasticsearch → Kibana
                          └→ Postgres (DLQ)

Airflow DAGs:
  - hourly_aggregation:   ES → aggregated metrics → Postgres
  - data_quality_check:   ES + Postgres counts → ratio metric → Postgres
  - dlq_reconcile:        Postgres DLQ → classification → Postgres metrics
```

For deeper architecture details, see the main [README.md](../README.md).
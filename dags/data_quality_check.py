from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
import psycopg2


ES_HOST = "http://elasticsearch:9200"
ES_INDEX = "ecommerce-events"
PG_CONN = {
    "host": "postgres",
    "port": 5432,
    "user": "ecom_user",
    "password": "enai.postgre",
    "database": "ecommerce",
}

DQ_THRESHOLD = 0.05


def measure_counts(**context):
    es = Elasticsearch([ES_HOST])
    es_count_result = es.count(index=ES_INDEX)
    es_count = es_count_result["count"]

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM ecommerce.dead_letter_events;")
    dlq_count = cur.fetchone()[0]
    cur.execute("""
        SELECT error_type, COUNT(*) 
        FROM ecommerce.dead_letter_events 
        GROUP BY error_type;
    """)
    dlq_breakdown = dict(cur.fetchall())
    cur.close()
    conn.close()

    total = es_count + dlq_count
    dq_ratio = (dlq_count / total) if total > 0 else 0.0

    print(f"[measure_counts] ES count: {es_count}")
    print(f"[measure_counts] DLQ count: {dlq_count}")
    print(f"[measure_counts] DLQ breakdown: {dlq_breakdown}")
    print(f"[measure_counts] Total events: {total}")
    print(f"[measure_counts] DQ ratio: {dq_ratio:.4f} ({dq_ratio * 100:.2f}%)")

    return {
        "es_count": es_count,
        "dlq_count": dlq_count,
        "total": total,
        "dq_ratio": dq_ratio,
        "dlq_breakdown": dlq_breakdown,
    }


def threshold_check(**context):
    data = context["ti"].xcom_pull(task_ids="measure_counts")

    if data["dq_ratio"] > DQ_THRESHOLD:
        print(f"[threshold_check] ⚠️ ALERT: DQ ratio {data['dq_ratio']:.4f} threshold {DQ_THRESHOLD} aşıyor!")
        alert_status = 1.0
    else:
        print(f"[threshold_check] ✓ DQ ratio {data['dq_ratio']:.4f} OK (threshold: {DQ_THRESHOLD}).")
        alert_status = 0.0
    metrics = [
        ("dq_es_count", float(data["es_count"])),
        ("dq_dlq_count", float(data["dlq_count"])),
        ("dq_total_events", float(data["total"])),
        ("dq_ratio", float(data["dq_ratio"])),
        ("dq_alert_status", alert_status),
    ]
    for error_type, count in data["dlq_breakdown"].items():
        metrics.append((f"dq_dlq.{error_type}", float(count)))

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO ecommerce.pipeline_metrics (metric_name, metric_value) VALUES (%s, %s)",
        metrics
    )
    conn.commit()
    inserted = cur.rowcount
    cur.close()
    conn.close()

    print(f"[threshold_check] {inserted} metric pipeline_metrics'e yazıldı.")


default_args = {
    "owner": "team_enai",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="data_quality_check",
    default_args=default_args,
    description="DLQ vs ES ratio hesabı + threshold alert + metric write",
    schedule_interval=None,  # production: '@hourly'
    start_date=datetime(2026, 5, 10),
    catchup=False,
    tags=["yzv322e", "monitoring", "dq"],
) as dag:

    measure_task = PythonOperator(
        task_id="measure_counts",
        python_callable=measure_counts,
    )

    check_task = PythonOperator(
        task_id="threshold_check",
        python_callable=threshold_check,
    )

    measure_task >> check_task

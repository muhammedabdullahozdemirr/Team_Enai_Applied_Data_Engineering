from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2

PG_CONN = {
    "host": "postgres",
    "port": 5432,
    "user": "ecom_user",
    "password": "enai.postgre",
    "database": "ecommerce",
}


def inspect_dlq(**context):
    """DLQ tablosunu incele, error_type bazında özet çıkar."""
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    # toplam ve breakdown
    cur.execute("SELECT COUNT(*) FROM ecommerce.dead_letter_events;")
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT error_type, COUNT(*), MIN(received_at), MAX(received_at)
        FROM ecommerce.dead_letter_events
        GROUP BY error_type
        ORDER BY COUNT(*) DESC;
    """)
    breakdown = cur.fetchall()

    # son 24 saatlik DLQ event'leri
    cur.execute("""
        SELECT COUNT(*) FROM ecommerce.dead_letter_events
        WHERE received_at > NOW() - INTERVAL '24 hours';
    """)
    recent_24h = cur.fetchone()[0]

    cur.close()
    conn.close()

    print(f"[inspect_dlq] Toplam DLQ event: {total}")
    print(f"[inspect_dlq] Son 24 saatte gelen: {recent_24h}")
    print(f"[inspect_dlq] Error type breakdown:")
    for error_type, count, first_seen, last_seen in breakdown:
        print(f"  {error_type}: {count} (first: {first_seen}, last: {last_seen})")

    # recovery candidate sayısını tahmin et
    # invalid_timestamp -> recoverable (timestamp düzeltilebilir)
    # negative_price -> reviewable (manuel inceleme gerekli)
    # null_user_id -> archivable (kalıcı arşivle)
    classification = {
        "recoverable": 0,
        "reviewable": 0,
        "archivable": 0,
    }
    for error_type, count, _, _ in breakdown:
        if error_type == "invalid_timestamp":
            classification["recoverable"] += count
        elif error_type == "negative_price":
            classification["reviewable"] += count
        elif error_type == "null_user_id":
            classification["archivable"] += count

    print(f"[inspect_dlq] Recovery classification: {classification}")
    return {"total": total, "recent_24h": recent_24h, "classification": classification}


def mark_unrecoverable(**context):
    """
    TODO — Production reconcile logic burada implement edilecek.
    Şu an sadece istatistik raporu pipeline_metrics'e yazıyor.
    """
    data = context["ti"].xcom_pull(task_ids="inspect_dlq")

    classification = data["classification"]
    metrics = [
        ("reconcile_total_dlq", float(data["total"])),
        ("reconcile_recent_24h", float(data["recent_24h"])),
        ("reconcile_recoverable", float(classification["recoverable"])),
        ("reconcile_reviewable", float(classification["reviewable"])),
        ("reconcile_archivable", float(classification["archivable"])),
    ]

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO ecommerce.pipeline_metrics (metric_name, metric_value) VALUES (%s, %s)",
        metrics
    )
    conn.commit()
    cur.close()
    conn.close()

    print(f"[mark_unrecoverable] {len(metrics)} reconcile metric yazıldı.")
    print(f"[mark_unrecoverable] NOT: Gerçek reconcile logic henüz implement edilmedi.")
    print(f"[mark_unrecoverable] Bkz: docstring TODO listesi.")


default_args = {
    "owner": "team_enai",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dlq_reconcile",
    default_args=default_args,
    description="DLQ event'lerini incele, recovery candidate'leri sınıflandır (skeleton)",
    schedule_interval=None,  # production: '@daily'
    start_date=datetime(2026, 5, 10),
    catchup=False,
    tags=["yzv322e", "dlq", "skeleton"],
) as dag:

    inspect_task = PythonOperator(
        task_id="inspect_dlq",
        python_callable=inspect_dlq,
    )

    mark_task = PythonOperator(
        task_id="mark_unrecoverable",
        python_callable=mark_unrecoverable,
    )

    inspect_task >> mark_task

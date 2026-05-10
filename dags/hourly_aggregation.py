from datetime import datetime, timedelta, timezone
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


def extract_from_es(**context):
    es = Elasticsearch([ES_HOST])
    now = datetime.now(timezone.utc)
    one_hour_ago = now - timedelta(hours=1)

    # ES sorgu: time range filter + multiple aggregations
    query = {
        "size": 0, 
        "query": {
            "range": {
                "event_timestamp": {
                    "gte": one_hour_ago.isoformat(),
                    "lte": now.isoformat()
                }
            }
        },
        "aggs": {
            "by_event_type": {"terms": {"field": "event_type.keyword", "size": 10}},
            "by_city": {"terms": {"field": "city.keyword", "size": 20}},
            "by_device": {"terms": {"field": "device_type.keyword", "size": 10}},
            "purchase_revenue": {
                "filter": {"term": {"event_type.keyword": "purchase"}},
                "aggs": {"total": {"sum": {"field": "cart_total_try"}}}
            }
        }
    }

    result = es.search(index=ES_INDEX, body=query)
    total_count = result["hits"]["total"]["value"]
    aggs = result["aggregations"]

    # metrikleri flat list'e topla
    metrics = []
    metrics.append(("events_total_last_hour", float(total_count)))

    for bucket in aggs["by_event_type"]["buckets"]:
        metrics.append((f"events_by_type.{bucket['key']}", float(bucket["doc_count"])))

    for bucket in aggs["by_city"]["buckets"]:
        metrics.append((f"events_by_city.{bucket['key']}", float(bucket["doc_count"])))

    for bucket in aggs["by_device"]["buckets"]:
        metrics.append((f"events_by_device.{bucket['key']}", float(bucket["doc_count"])))

    revenue = aggs["purchase_revenue"]["total"]["value"] or 0.0
    metrics.append(("purchase_revenue_try", float(revenue)))

    print(f"[extract_from_es] Penceere: {one_hour_ago.isoformat()} → {now.isoformat()}")
    print(f"[extract_from_es] Toplam event: {total_count}")
    print(f"[extract_from_es] Hesaplanan metrik sayısı: {len(metrics)}")
    for name, value in metrics:
        print(f"  {name}: {value}")

    return metrics


def load_to_postgres(**context):
    metrics = context["ti"].xcom_pull(task_ids="extract_from_es")
    if not metrics:
        print("[load_to_postgres] Metric yok, çıkıyorum.")
        return

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    insert_sql = """
        INSERT INTO ecommerce.pipeline_metrics (metric_name, metric_value)
        VALUES (%s, %s)
    """
    cur.executemany(insert_sql, metrics)
    conn.commit()
    inserted = cur.rowcount
    cur.close()
    conn.close()

    print(f"[load_to_postgres] Eklenen satır: {inserted}")


default_args = {
    "owner": "team_enai",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="hourly_aggregation",
    default_args=default_args,
    description="ES son 1 saatlik event'leri aggregate eder, Postgres pipeline_metrics'e yazar",
    schedule_interval=None,  # production'da: '@hourly' yapılabilir
    start_date=datetime(2026, 5, 10),
    catchup=False,
    tags=["yzv322e", "aggregation", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_from_es",
        python_callable=extract_from_es,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    extract_task >> load_task

# Postgreden products tablosunu okur

import os
import time
import logging
import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)


def fetch_products(retries=30, delay=2):
    params = {
        "host": os.environ.get("POSTGRES_HOST", "postgres"),
        "port": 5432,
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "dbname": os.environ["POSTGRES_DB"],
    }
    for attempt in range(1, retries + 1):
        try:
            with psycopg2.connect(**params) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT product_id, product_category, base_price_try
                        FROM ecommerce.products ORDER BY product_id
                    """)
                    rows = [dict(r) for r in cur.fetchall()]
                    for r in rows:
                        r["base_price_try"] = float(r["base_price_try"])
                    log.info("Postgres'ten %d ürün çekildi", len(rows))
                    return rows
        except psycopg2.OperationalError as e:
            log.warning("Postgres henüz hazır değil (%d/%d): %s", attempt, retries, e)
            time.sleep(delay)
    raise RuntimeError("Postgres'e bağlanılamadı")

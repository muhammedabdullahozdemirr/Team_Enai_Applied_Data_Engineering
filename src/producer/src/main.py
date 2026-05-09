# Postgres'ten ürünleri çek --> generator kur --> Kafka'ya gönder --> stat bas.

import logging
import os
import signal
import sys
import time
from collections import Counter

from .generator import EventGenerator
from .kafka_client import KafkaSender
from .products_repo import fetch_products

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ecommerce-events")
EVENTS_PER_SECOND = int(os.environ.get("EVENTS_PER_SECOND", "100"))
TOTAL_EVENTS = int(os.environ.get("TOTAL_EVENTS", "30000"))
NUM_USERS = int(os.environ.get("NUM_USERS", "2000"))
DQ_RATE = float(os.environ.get("DQ_ISSUE_RATE", "0.025"))
LOG_INTERVAL = 10  

stop = False
def handle_signal(signum, frame):
    global stop
    stop = True

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("producer")

    log.info("=" * 60)
    log.info("YZV 322E Producer | bootstrap=%s topic=%s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    log.info("Total=%d, rate=%d/s, users=%d, dq=%.1f%%",
             TOTAL_EVENTS, EVENTS_PER_SECOND, NUM_USERS, DQ_RATE * 100)
    log.info("=" * 60)

    products = fetch_products()
    if not products:
        log.error("Products tablosu boş!")
        return 2

    generator = EventGenerator(products, NUM_USERS, DQ_RATE)
    sender = KafkaSender(KAFKA_BOOTSTRAP, KAFKA_TOPIC)

    interval = 1.0 / EVENTS_PER_SECOND
    next_send = time.monotonic()
    next_log = time.monotonic() + LOG_INTERVAL
    event_types = Counter()
    dq_types = Counter()
    start = time.monotonic()

    try:
        for i in range(TOTAL_EVENTS):
            if stop:
                log.info("Shutdown istendi, %d/%d event'te durdum", i, TOTAL_EVENTS)
                break

            event, dq = generator.generate_one()
            event_types[event["event_type"]] += 1
            if dq:
                dq_types[dq] += 1

            sender.send(event, key=event["session_id"])

            next_send += interval
            wait = next_send - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            else:
                next_send = time.monotonic()

            now = time.monotonic()
            if now >= next_log:
                rate = (i + 1) / (now - start)
                log.info("Progress: %d/%d (%.1f%%) | rate=%.1f/s | delivered=%d failed=%d",
                         i + 1, TOTAL_EVENTS, 100 * (i + 1) / TOTAL_EVENTS,
                         rate, sender.delivered, sender.failed)
                next_log = now + LOG_INTERVAL
    finally:
        log.info("Flush ediliyor...")
        remaining = sender.flush(30)
        if remaining:
            log.warning("%d mesaj flush edilemedi", remaining)

        elapsed = time.monotonic() - start
        total = sum(event_types.values())
        log.info("=" * 60)
        log.info("BITTI | %d event in %.1fs (%.1f/s)", total, elapsed,
                 total / elapsed if elapsed else 0)
        log.info("Delivered=%d Failed=%d", sender.delivered, sender.failed)
        for et, cnt in event_types.most_common():
            log.info("  %-18s %5d (%.1f%%)", et, cnt, 100 * cnt / total)
        if dq_types:
            log.info("DQ injected:")
            for dt, cnt in dq_types.most_common():
                log.info("  %-18s %5d", dt, cnt)
        log.info("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())

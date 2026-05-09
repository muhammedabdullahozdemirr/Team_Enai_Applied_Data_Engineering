import json
import logging
from confluent_kafka import Producer

log = logging.getLogger(__name__)


class KafkaSender:
    def __init__(self, bootstrap, topic):
        self.topic = topic
        self.delivered = 0
        self.failed = 0
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": "yzv322e-producer",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "snappy",
            "linger.ms": 10,
            "batch.size": 32 * 1024,
        }, logger=log)

    def _on_delivery(self, err, msg):
        if err is not None:
            self.failed += 1
            log.error("Delivery failed: %s", err)
        else:
            self.delivered += 1

    def send(self, event, key=None):
        try:
            self.producer.produce(
                topic=self.topic,
                value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                key=key.encode("utf-8") if key else None,
                callback=self._on_delivery,
            )
            self.producer.poll(0)
        except BufferError:
            log.warning("Queue full, flush ediliyor")
            self.producer.flush(5)
            self.producer.produce(
                topic=self.topic,
                value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                key=key.encode("utf-8") if key else None,
                callback=self._on_delivery,
            )

    def flush(self, timeout=30):
        return self.producer.flush(timeout)

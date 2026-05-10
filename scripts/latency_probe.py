#!/usr/bin/env python3
# end to end latency probe.
# producer event basarken ES yi poll eder her event icin
# event_timestamp ile ES e dustugu an arasindaki farki olcer.
# p50 p95 p99 mean max yazar bench/latency.csv ye.
#
# kullanim:
#   1. compose ayakta olsun NiFi flow ENABLED
#   2. ortami temizle:
#        curl -s -X DELETE "http://localhost:9200/ecommerce-events"
#   3. producer baslat (ayri terminalde):
#        docker compose up producer
#   4. bunu paralel calistir:
#        python3 scripts/latency_probe.py

import time
import json
import urllib.request
import statistics
from datetime import datetime, timezone

ES_URL = "http://localhost:9200/ecommerce-events/_search"
POLL_INTERVAL = 0.5  # sn
MAX_WAIT_AFTER_LAST = 30  # son event geldikten kac sn sonra durduralim
TARGET_SAMPLES = 5000   # en fazla bu kadar event icin latency olcumu


def es_query(after_timestamp_ms, size=1000):
    """ES ten event_timestamp > after_ts olanlari cek, sort artan."""
    body = {
        "size": size,
        "sort": [{"event_timestamp": "asc"}],
        "query": {
            "range": {
                "event_timestamp": {
                    "gt": after_timestamp_ms,
                    "format": "epoch_millis"
                }
            }
        },
        "_source": ["event_id", "event_timestamp"]
    }
    req = urllib.request.Request(
        ES_URL,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def parse_iso(ts):
    """generator iso format: 2026-05-10T22:00:00.000Z"""
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def main():
    print("=" * 60)
    print("End-to-End Latency Probe")
    print("=" * 60)
    print(f"ES: {ES_URL}")
    print(f"Poll: {POLL_INTERVAL}s, target samples: {TARGET_SAMPLES}")
    print("Bekleniyor... (producer baslat: docker compose up producer)")
    print()

    seen = set()
    samples = []  # (event_id, latency_sec, event_ts_iso, observed_at_iso)
    last_seen_ms = 0
    last_new_event_time = time.monotonic()
    start_time = time.monotonic()

    while True:
        try:
            result = es_query(last_seen_ms)
        except Exception as e:
            # ES daha hazir degil olabilir
            print(f"  [warn] ES query failed: {e}")
            time.sleep(POLL_INTERVAL * 2)
            continue

        hits = result.get("hits", {}).get("hits", [])
        now = datetime.now(timezone.utc)
        new_count = 0

        for hit in hits:
            src = hit.get("_source", {})
            eid = src.get("event_id")
            ets = src.get("event_timestamp")
            if not eid or not ets or eid in seen:
                continue
            seen.add(eid)

            try:
                event_dt = parse_iso(ets)
            except Exception:
                continue

            latency = (now - event_dt).total_seconds()
            # negatif latency olamaz event_timestamp gelecekteyse ignore
            # (bizim DQ injection invalid_timestamp=2099 onun icin)
            if latency < 0 or latency > 3600:
                continue

            samples.append((eid, latency, ets, now.isoformat()))
            new_count += 1

            # son seen event_ts i ms cinsinden tut bir sonraki query icin
            ts_ms = int(event_dt.timestamp() * 1000)
            if ts_ms > last_seen_ms:
                last_seen_ms = ts_ms

        if new_count > 0:
            last_new_event_time = time.monotonic()
            print(f"  [{int(time.monotonic() - start_time):4d}s] yeni: {new_count:4d} | toplam: {len(samples):5d} | son latency: {samples[-1][1]:6.2f}s")

        # cikis kosullari
        if len(samples) >= TARGET_SAMPLES:
            print(f"\n[done] {TARGET_SAMPLES} sample tamam, durduruluyor.")
            break
        if time.monotonic() - last_new_event_time > MAX_WAIT_AFTER_LAST:
            print(f"\n[done] {MAX_WAIT_AFTER_LAST}s yeni event yok, durduruluyor.")
            break

        time.sleep(POLL_INTERVAL)

    if not samples:
        print("\nHic sample toplanamadi.")
        return 1

    # ozet
    latencies = [s[1] for s in samples]
    latencies.sort()
    n = len(latencies)
    p50 = latencies[n // 2]
    p95 = latencies[int(n * 0.95)]
    p99 = latencies[int(n * 0.99)]
    mean = statistics.mean(latencies)
    stdev = statistics.stdev(latencies) if n > 1 else 0.0
    mn = latencies[0]
    mx = latencies[-1]

    print()
    print("=" * 60)
    print(f"Latency Summary (n={n})")
    print("=" * 60)
    print(f"  min     : {mn:7.3f} s")
    print(f"  p50     : {p50:7.3f} s")
    print(f"  mean    : {mean:7.3f} s")
    print(f"  p95     : {p95:7.3f} s")
    print(f"  p99     : {p99:7.3f} s")
    print(f"  max     : {mx:7.3f} s")
    print(f"  stdev   : {stdev:7.3f} s")
    print("=" * 60)

    # bench/latency.csv ye yaz
    import os
    os.makedirs("bench", exist_ok=True)
    with open("bench/latency.csv", "w") as f:
        f.write("event_id,latency_sec,event_timestamp,observed_at\n")
        for eid, lat, ets, obs in samples:
            f.write(f"{eid},{lat:.4f},{ets},{obs}\n")
    print(f"\nDetay: bench/latency.csv ({n} satir)")

    # summary text
    with open("bench/latency_summary.txt", "w") as f:
        f.write(f"n={n}\n")
        f.write(f"min={mn:.3f}\n")
        f.write(f"p50={p50:.3f}\n")
        f.write(f"mean={mean:.3f}\n")
        f.write(f"p95={p95:.3f}\n")
        f.write(f"p99={p99:.3f}\n")
        f.write(f"max={mx:.3f}\n")
        f.write(f"stdev={stdev:.3f}\n")
    print(f"Ozet: bench/latency_summary.txt")

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

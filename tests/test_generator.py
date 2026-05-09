# Generator ın doğru event ürettiğini localde kafka olmadan test etmek için

import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "producer"))
from src.generator import EventGenerator


def fake_products(n=50):
    cats = ["electronics", "fashion", "home", "books", "sports"]
    return [{"product_id": f"prod_{i:05d}",
             "product_category": cats[i % 5],
             "base_price_try": 100.0 + i * 10} for i in range(n)]


def main():
    print("Generator smoke test(5000 event)")

    gen = EventGenerator(fake_products(50), num_users=200, dq_rate=0.025, seed=42)

    types, dqs, cities, devices = Counter(), Counter(), Counter(), Counter()
    sessions = set()
    null_users = 0

    for i in range(5000):
        ev, dq = gen.generate_one()
        types[ev["event_type"]] += 1
        sessions.add(ev["session_id"])
        cities[ev["city"]] += 1
        devices[ev["device_type"]] += 1
        if ev["user_id"] is None:
            null_users += 1
        if dq:
            dqs[dq] += 1
        if i < 2:
            print(f"\nÖrnek event #{i+1}:")
            print(json.dumps(ev, indent=2, ensure_ascii=False))

    print("\n--- event type ---")
    for t, c in types.most_common():
        print(f"  {t:18s} {c:5d}  ({100*c/5000:5.1f}%)")

    print(f"\n--- DQ injected: {sum(dqs.values())} ({100*sum(dqs.values())/5000:.1f}%) ---")
    for t, c in dqs.most_common():
        print(f"  {t:18s} {c:5d}")

    print(f"\nUnique sessions: {len(sessions)}")
    print(f"Null user_id events: {null_users} ({100*null_users/5000:.1f}%)")
    print(f"İstanbul payı--> {100*cities['İstanbul']/5000:.1f}%")
    print(f"Mobil payı--> {100*devices['mobile']/5000:.1f}%")

    print("\n--- Sanity ---")
    pv = 100 * types["page_view"] / 5000
    pur = 100 * types["purchase"] / 5000
    ist = 100 * cities["İstanbul"] / 5000
    mob = 100 * devices["mobile"] / 5000
    dq_pct = 100 * sum(dqs.values()) / 5000

    checks = [
        ("page_view dominant (>50%)", pv > 50, f"{pv:.1f}%"),
        ("purchase rare (<5%)", pur < 5, f"{pur:.1f}%"),
        ("İstanbul ~35%", 28 < ist < 42, f"{ist:.1f}%"),
        ("mobile ~60%", 50 < mob < 70, f"{mob:.1f}%"),
        ("DQ ~2.5%", 1.5 < dq_pct < 3.5, f"{dq_pct:.1f}%"),
    ]
    ok = True
    for name, passed, val in checks:
        sign = "ok" if passed else "fail"
        print(f"[{sign}] {name}: {val}")
        if not passed:
            ok = False
    print("=" * 10)
    print("testlerin hepsi okey" if ok else "geçemeyen testler var!!")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

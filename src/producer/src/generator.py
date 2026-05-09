# sentetik event üretici.

import random
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

CITIES = [
    ("İstanbul", 0.35), ("Ankara", 0.15), ("İzmir", 0.10), ("Bursa", 0.07),
    ("Antalya", 0.06), ("Adana", 0.05), ("Konya", 0.04), ("Gaziantep", 0.04),
    ("Şanlıurfa", 0.03), ("Kayseri", 0.03), ("Diğer", 0.08),
]
DEVICES = [("mobile", 0.60), ("desktop", 0.32), ("tablet", 0.08)]
BROWSERS = [("chrome", 0.55), ("safari", 0.25), ("firefox", 0.10), ("edge", 0.10)]
REFERRERS = [("google", 0.35), ("direct", 0.30), ("instagram", 0.15),("facebook", 0.10), ("email", 0.10)]
PAYMENTS = [("credit_card", 0.70), ("bank_transfer", 0.15), ("wallet", 0.15)]

P_VIEW_TO_CLICK = 0.15
P_CLICK_TO_CART = 0.20
P_CART_TO_CHECKOUT = 0.40
P_CHECKOUT_TO_PURCHASE = 0.60

SEARCH_TERMS = ["telefon", "laptop", "kulaklık", "ayakkabı", "kitap", "elbise", "halı", "bisiklet", "tablet", "monitor", "çanta", "saksı"]

def pick(weighted):
    items, weights = zip(*weighted)
    return random.choices(items, weights=weights, k=1)[0]


def now_iso(offset_min=0):
    t = datetime.utcnow() + timedelta(minutes=offset_min, milliseconds=random.randint(0, 999))
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z"


def make_user_pool(num_users):
    """Pareto: ilk %20 kullanıcı havuza 16 kez girer, %80 trafik onlardan."""
    pool = []
    heavy = max(1, num_users // 5)
    for i in range(heavy):
        pool.extend([f"user_{i:06d}"] * 16)
    for i in range(heavy, num_users):
        pool.append(f"user_{i:06d}")
    return pool


@dataclass
class Session:
    session_id: str
    user_id: str  # None olabilir (anonim)
    city: str
    device_type: str
    browser: str
    referrer: str
    cart: list  # [{product_id, category, price, qty, discount}]
    last_event: str  #son event_type
    count: int  #session içi event sayısı


def new_session(user_pool, anonymous=False):
    return Session(
        session_id=f"sess_{uuid.uuid4().hex[:8]}",
        user_id=None if anonymous else random.choice(user_pool),
        city=pick(CITIES),
        device_type=pick(DEVICES),
        browser=pick(BROWSERS),
        referrer=pick(REFERRERS),
        cart=[],
        last_event=None,
        count=0,
    )


def base_event(session, event_type, ts):
    """tüm eventlerde ortak alanlar."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": ts,
        "session_id": session.session_id,
        "user_id": session.user_id,
        "city": session.city,
        "device_type": session.device_type,
        "browser": session.browser,
        "referrer": session.referrer,
        "product_id": None,
        "product_category": None,
        "product_price_try": None,
        "quantity": None,
        "discount_pct": None,
        "cart_total_try": None,
        "payment_method": None,
        "search_query": None,
    }


def next_event_type(last):
    """Funnel state machine-önceki evente göre olası sonraki."""
    if last is None or last == "purchase":
        return "page_view"
    if last == "page_view":
        return "product_click" if random.random() < P_VIEW_TO_CLICK else "page_view"
    if last == "product_click":
        return "add_to_cart" if random.random() < P_CLICK_TO_CART else "page_view"
    if last == "add_to_cart":
        return "checkout_start" if random.random() < P_CART_TO_CHECKOUT else "page_view"
    if last == "checkout_start":
        return "purchase" if random.random() < P_CHECKOUT_TO_PURCHASE else "page_view"
    return "page_view"


def build_event(session, products, ts):
    """Bir event üret. Session stateini günceller."""
    et = next_event_type(session.last_event)
    ev = base_event(session, et, ts)

    if et == "page_view":
        # yüzde 60 ihtimal bir ürün sayfası %20 ihtimal search
        if random.random() < 0.60:
            p = random.choice(products)
            ev["product_id"] = p["product_id"]
            ev["product_category"] = p["product_category"]
            ev["product_price_try"] = round(p["base_price_try"], 2)
        if random.random() < 0.20:
            ev["search_query"] = random.choice(SEARCH_TERMS)

    elif et == "product_click":
        p = random.choice(products)
        ev["product_id"] = p["product_id"]
        ev["product_category"] = p["product_category"]
        ev["product_price_try"] = round(p["base_price_try"], 2)

    elif et == "add_to_cart":
        p = random.choice(products)
        qty = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
        disc = round(random.uniform(0, 0.30), 2) if random.random() < 0.30 else 0.0
        price = round(p["base_price_try"], 2)
        session.cart.append({
            "product_id": p["product_id"], "category": p["product_category"],
            "price": price, "qty": qty, "discount": disc,
        })
        ev["product_id"] = p["product_id"]
        ev["product_category"] = p["product_category"]
        ev["product_price_try"] = price
        ev["quantity"] = qty
        ev["discount_pct"] = disc if disc > 0 else None

    elif et == "checkout_start":
        if not session.cart:
            return build_event(session, products, ts)
        total = sum(it["price"] * it["qty"] * (1 - it["discount"]) for it in session.cart)
        ev["cart_total_try"] = round(total, 2)
        ev["quantity"] = sum(it["qty"] for it in session.cart)

    elif et == "purchase":
        if not session.cart:
            return build_event(session, products, ts)
        total = sum(it["price"] * it["qty"] * (1 - it["discount"]) for it in session.cart)
        ev["cart_total_try"] = round(total, 2)
        ev["quantity"] = sum(it["qty"] for it in session.cart)
        ev["payment_method"] = pick(PAYMENTS)
        session.cart = []

    session.last_event = et
    session.count += 1
    return ev


def inject_dq_issue(event):
    """Bilerek bozuk event üret."""
    issue = random.choices(
        ["null_user_id", "invalid_timestamp", "negative_price"],
        weights=[0.6, 0.25, 0.15], k=1,
    )[0]
    if issue == "null_user_id":
        event["user_id"] = None
    elif issue == "invalid_timestamp":
        event["event_timestamp"] = "2099-01-01T00:00:00.000Z"
    elif issue == "negative_price" and event.get("product_price_try"):
        event["product_price_try"] = -abs(event["product_price_try"])
    return event, issue


class EventGenerator:
    def __init__(self, products, num_users, dq_rate, seed=None):
        if seed is not None:
            random.seed(seed)
        self.products = products
        self.user_pool = make_user_pool(num_users)
        self.dq_rate = dq_rate
        self.sessions = []
        self.emitted = 0

    def _get_session(self):
        #timeout simülasyonu
        self.sessions = [s for s in self.sessions if s.count < 30]
        if self.sessions and random.random() < 0.70:
            return random.choice(self.sessions)
        anon = random.random() < 0.015 
        s = new_session(self.user_pool, anonymous=anon)
        self.sessions.append(s)
        return s

    def generate_one(self):
        session = self._get_session()
        ts = now_iso(offset_min=self.emitted // 100)
        event = build_event(session, self.products, ts)
        self.emitted += 1

        dq_type = None
        if random.random() < self.dq_rate:
            event, dq_type = inject_dq_issue(event)
        return event, dq_type

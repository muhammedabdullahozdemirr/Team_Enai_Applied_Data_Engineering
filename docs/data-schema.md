# Event Schema

`ecommerce-events` topic'ine basılan json eventlerin yapısı

## example

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "purchase",
  "event_timestamp": "2026-05-09T14:23:11.342Z",
  "session_id": "sess_a1b2c3d4",
  "user_id": "user_001234",
  "city": "İstanbul",
  "device_type": "mobile",
  "browser": "chrome",
  "referrer": "instagram",
  "product_id": "prod_00567",
  "product_category": "electronics",
  "product_price_try": 149.99,
  "quantity": 2,
  "discount_pct": 0.10,
  "cart_total_try": 269.98,
  "payment_method": "credit_card",
  "search_query": null
}
```

## Event tipleri

`page_view`, `product_click`, `add_to_cart`, `checkout_start`, `purchase`

## Funnel oranları

```
page_view (100%)
  -> product_click (15%)
       -> add_to_cart (20%)
            -> checkout_start (40%)
                 -> purchase (60%)
```

Yani 100 page_view -> ~0.7 purchase. TR e-commerce conversion rate'i ~%1, bizim funnelımız bu aralıkta.

## Dağılımlar

- **city**: 10 büyük TR şehri + diğer.
- **device_type**: mobile %60, desktop %32, tablet %8.
- **browser**: chrome %55, safari %25.
- **referrer**: google %35, direct %30, instagram %15.
- **user**: Pareto — %20 user, %80 trafik.

## DQ injection (%2.5 oranında)

Generator bilerek bozuk kayıt üretiyor, Nifi yakalayıp Postgres dead_letter_eventse yazacak.

| Hata | Oran |
|---|---|
| `user_id` null | %1.5 |
| `event_timestamp` invalid (2099) | %0.6 |
| `product_price_try` negatif | %0.4 |

## NiFi enrichment

NiFi event'i ES'e yazmadan önce Postgres'teki `products` tablosundan şu alanları lookup'lar:
- `product_brand`
- `stock_level`
- `is_featured`

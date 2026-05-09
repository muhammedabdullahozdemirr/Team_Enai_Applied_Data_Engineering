SET search_path TO ecommerce, public;

INSERT INTO ecommerce.products (product_id, product_name, product_category, product_brand, stock_level, is_featured, base_price_try)
SELECT
    'prod_' || LPAD(s::text, 5, '0'),
    CASE category
        WHEN 'electronics' THEN (ARRAY['Telefon','Laptop','Kulaklık','Tablet','TV','Monitor','Klavye','Mouse'])[1 + (s % 8)]
        WHEN 'fashion'     THEN (ARRAY['Tişört','Pantolon','Ceket','Ayakkabı','Çanta','Şapka','Elbise','Kazak'])[1 + (s % 8)]
        WHEN 'home'        THEN (ARRAY['Lamba','Halı','Yastık','Battaniye','Vazo','Tablo','Mum','Saksı'])[1 + (s % 8)]
        WHEN 'books'       THEN (ARRAY['Roman','Şiir','Tarih','Felsefe','Bilim','Çocuk','Polisiye','Biyografi'])[1 + (s % 8)]
        WHEN 'sports'      THEN (ARRAY['Top','Forma','Eldiven','Bisiklet','Yoga Matı','Dumbell','Mayo','Raket'])[1 + (s % 8)]
    END || ' ' || (s % 100 + 1)::text,
    category,
    (ARRAY['Samsung','Apple','Nike','Adidas','LCWaikiki','Defacto','Vakkorama','Penguen','Migros','Sony'])[1 + (s % 10)],
    CASE WHEN random() < 0.05 THEN 0 ELSE 50 + (random() * 450)::int END,
    random() < 0.15,
    CASE category
        WHEN 'electronics' THEN 500 + random() * 49500
        WHEN 'fashion'     THEN 100 + random() * 2900
        WHEN 'home'        THEN 50  + random() * 1950
        WHEN 'books'       THEN 30  + random() * 270
        WHEN 'sports'      THEN 100 + random() * 4900
    END::NUMERIC(10, 2)
FROM
    generate_series(1, 500) AS s,
    LATERAL (SELECT (ARRAY['electronics','fashion','home','books','sports'])[1 + ((s - 1) / 100)] AS category) cat;

CREATE SCHEMA IF NOT EXISTS ecommerce;
SET search_path TO ecommerce, public;

-- ürün master tablosu.nifi enrichment için bu tabloya bakcak
CREATE TABLE ecommerce.products (
    product_id       VARCHAR(20) PRIMARY KEY,
    product_name     VARCHAR(200) NOT NULL,
    product_category VARCHAR(50)  NOT NULL,
    product_brand    VARCHAR(100),
    stock_level      INTEGER DEFAULT 0,
    is_featured      BOOLEAN DEFAULT FALSE,
    base_price_try   NUMERIC(10, 2) NOT NULL
);

-- bozuk eventler buraya.geçersiz kayıtlsr buraya yazılcak
CREATE TABLE ecommerce.dead_letter_events (
    dlq_id         BIGSERIAL PRIMARY KEY,
    received_at    TIMESTAMPTZ DEFAULT NOW(),
    original_event JSONB NOT NULL,
    error_type     VARCHAR(100) NOT NULL,
    error_detail   TEXT
);

CREATE INDEX idx_dlq_error ON ecommerce.dead_letter_events(error_type);
CREATE INDEX idx_dlq_event_gin ON ecommerce.dead_letter_events USING GIN (original_event);

CREATE TABLE ecommerce.pipeline_metrics (
    metric_id    BIGSERIAL PRIMARY KEY,
    metric_name  VARCHAR(100) NOT NULL,
    metric_value NUMERIC(20, 4) NOT NULL,
    measured_at  TIMESTAMPTZ DEFAULT NOW()
);

-- BRONZE VEGETABLE PRICES TABLE SCHEMA
-- Raw data layer for vegetable wastage analytics

CREATE TABLE bronze_vegetable_prices (
    id SERIAL PRIMARY KEY,
    raw JSONB,                           -- Complete Kafka message
    report_date DATE,                   -- Date of report
    vegetable TEXT,                     -- Vegetable type
    quality_grade TEXT,                 -- Quality grade (A, B, C)
    season TEXT,                        -- Season (Winter, Summer, etc.)
    top_district TEXT,                  -- District name
    production_kg NUMERIC,              -- Total production in KG
    wasted_kg NUMERIC,                  -- Wastage in KG
    wastage_percent NUMERIC,            -- Wastage percentage
    market_price NUMERIC,               -- Market price
    buyer_price NUMERIC,                -- Buyer price  
    farmer_price NUMERIC,               -- Farmer price
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW(), -- Ingestion timestamp
    
    -- Additional fields from CSV
    actual_price_diff NUMERIC,
    avg_temp_c NUMERIC,
    avg_humidity NUMERIC,
    transport_cost_per_kg NUMERIC,
    wastage_cost NUMERIC,
    avoidable_wastage_kg_10pct NUMERIC,
    avoidable_loss_inr_10pct NUMERIC,
    is_monsoon BOOLEAN,
    item_type TEXT,
    price_source TEXT,
    export_demand TEXT,
    wastage_reasons TEXT,
    validation_links TEXT
);

-- Indexes for better performance
CREATE INDEX idx_bronze_vegetable ON bronze_vegetable_prices(vegetable);
CREATE INDEX idx_bronze_district ON bronze_vegetable_prices(top_district);
CREATE INDEX idx_bronze_date ON bronze_vegetable_prices(report_date);

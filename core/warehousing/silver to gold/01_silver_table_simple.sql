-- SILVER VEGETABLE PRICES TABLE (INTERMEDIATE FRIENDLY)
-- Cleaned and validated data from Bronze layer
-- SIMPLE CONSTRAINTS: Easy to understand and debug

CREATE TABLE silver_vegetable_prices (
    id SERIAL PRIMARY KEY,
    bronze_id INTEGER UNIQUE REFERENCES bronze_vegetable_prices(id),
    
    -- Core business fields (cleaned)
    report_date DATE NOT NULL,
    vegetable VARCHAR(100) NOT NULL,
    top_district VARCHAR(100) NOT NULL,
    season VARCHAR(50),
    quality_grade VARCHAR(10),

    -- Quantitative fields (basic validation)
    production_kg DECIMAL(12,2) CHECK (production_kg >= 0),
    wasted_kg DECIMAL(12,2) CHECK (wasted_kg >= 0),
    wastage_percent DECIMAL(5,2) CHECK (wastage_percent >= 0 AND wastage_percent <= 100),

    -- Price fields (basic validation)
    farmer_price DECIMAL(8,2) CHECK (farmer_price >= 0),
    market_price DECIMAL(8,2) CHECK (market_price >= 0),
    buyer_price DECIMAL(8,2) CHECK (buyer_price >= 0),

    -- Calculated fields (no constraints - we'll calculate them)
    price_gap_market_farmer DECIMAL(8,2),
    price_gap_market_buyer DECIMAL(8,2),
    total_wastage_cost DECIMAL(12,2),

    -- Quality flags (simple business rules)
    is_high_wastage BOOLEAN DEFAULT FALSE,
    is_price_anomaly BOOLEAN DEFAULT FALSE,
    is_data_complete BOOLEAN DEFAULT TRUE,

    -- Metadata
    cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score INTEGER DEFAULT 100
);

-- Simple indexes for performance
CREATE INDEX idx_silver_vegetable ON silver_vegetable_prices(vegetable);
CREATE INDEX idx_silver_date ON silver_vegetable_prices(report_date);

COMMENT ON TABLE silver_vegetable_prices IS 'Cleaned vegetable wastage data - Intermediate friendly version';
COMMENT ON COLUMN silver_vegetable_prices.is_high_wastage IS 'Simple flag: TRUE if wastage > 30%';

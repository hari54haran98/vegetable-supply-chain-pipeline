-- SILVER VEGETABLE PRICES TABLE
-- Cleaned, validated, and enriched data from Bronze layer

CREATE TABLE IF NOT EXISTS silver_vegetable_prices (
    id SERIAL PRIMARY KEY,
    bronze_id INTEGER REFERENCES bronze_vegetable_prices(id),
    
    -- Core business fields (cleaned)
    report_date DATE NOT NULL,
    vegetable VARCHAR(100) NOT NULL,
    top_district VARCHAR(100) NOT NULL,
    season VARCHAR(50),
    quality_grade VARCHAR(10),
    
    -- Quantitative fields (validated)
    production_kg DECIMAL(12,2) CHECK (production_kg >= 0),
    wasted_kg DECIMAL(12,2) CHECK (wasted_kg >= 0),
    wastage_percent DECIMAL(5,2) CHECK (wastage_percent >= 0 AND wastage_percent <= 100),
    
    -- Price fields (standardized)
    farmer_price DECIMAL(8,2) CHECK (farmer_price >= 0),
    market_price DECIMAL(8,2) CHECK (market_price >= 0),
    buyer_price DECIMAL(8,2) CHECK (buyer_price >= 0),
    
    -- Calculated fields
    price_gap_market_farmer DECIMAL(8,2),
    price_gap_market_buyer DECIMAL(8,2),
    total_wastage_cost DECIMAL(12,2),
    
    -- Quality flags
    is_high_wastage BOOLEAN DEFAULT FALSE,
    is_price_anomaly BOOLEAN DEFAULT FALSE,
    is_data_complete BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score INTEGER DEFAULT 100,
    
    -- Constraints
    UNIQUE(bronze_id),
    CHECK (market_price >= farmer_price), -- Business rule validation
    CHECK (wastage_percent = ROUND((wasted_kg / production_kg) * 100, 2)) -- Calculation validation
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_silver_vegetable ON silver_vegetable_prices(vegetable);
CREATE INDEX IF NOT EXISTS idx_silver_district ON silver_vegetable_prices(top_district);
CREATE INDEX IF NOT EXISTS idx_silver_date ON silver_vegetable_prices(report_date);
CREATE INDEX IF NOT EXISTS idx_silver_quality ON silver_vegetable_prices(data_quality_score);

COMMENT ON TABLE silver_vegetable_prices IS 'Cleaned and validated vegetable wastage data with quality checks';
COMMENT ON COLUMN silver_vegetable_prices.is_high_wastage IS 'TRUE if wastage_percent > 30%';
COMMENT ON COLUMN silver_vegetable_prices.data_quality_score IS 'Data quality score (0-100) based on completeness and validation';

-- SILVER VEGETABLE PRICES TABLE (FIXED CONSTRAINTS)
-- Cleaned and validated data from Bronze layer
-- FIXED: Relaxed constraints for real-world data rounding differences

DROP TABLE IF EXISTS silver_vegetable_prices CASCADE;

CREATE TABLE silver_vegetable_prices (
    id SERIAL PRIMARY KEY,
    bronze_id INTEGER UNIQUE REFERENCES bronze_vegetable_prices(id),
    
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

    -- Quality flags (INTERMEDIATE-FRIENDLY: Simple business rules)
    is_high_wastage BOOLEAN DEFAULT FALSE,
    is_price_anomaly BOOLEAN DEFAULT FALSE,
    is_data_complete BOOLEAN DEFAULT TRUE,

    -- Metadata
    cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score INTEGER DEFAULT 100 CHECK (data_quality_score >= 0 AND data_quality_score <= 100),

    -- FIXED CONSTRAINTS: Allow small rounding differences
    -- Instead of exact match, allow ±0.5% difference for real-world data
    CHECK (
        ABS(wastage_percent - ROUND((wasted_kg / NULLIF(production_kg, 0)) * 100, 2)) <= 0.5
        OR production_kg = 0
    ),
    
    -- Business rule: Market price should generally be higher than farmer price
    -- But allow exceptions for data quality issues
    CHECK (market_price >= farmer_price * 0.8) -- Allow 20% variance for data issues
);

-- Performance indexes
CREATE INDEX idx_silver_vegetable ON silver_vegetable_prices(vegetable);
CREATE INDEX idx_silver_district ON silver_vegetable_prices(top_district);
CREATE INDEX idx_silver_date ON silver_vegetable_prices(report_date);
CREATE INDEX idx_silver_quality ON silver_vegetable_prices(data_quality_score);

COMMENT ON TABLE silver_vegetable_prices IS 'Cleaned and validated vegetable wastage data with realistic constraints';
COMMENT ON COLUMN silver_vegetable_prices.is_high_wastage IS 'TRUE if wastage_percent > 30% (business rule)';
COMMENT ON COLUMN silver_vegetable_prices.data_quality_score IS 'Data quality score (0-100) based on completeness and validation rules';

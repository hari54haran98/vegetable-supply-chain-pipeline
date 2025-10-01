-- GOLD LAYER: Business metrics and ML predictions
CREATE TABLE gold_vegetable_metrics (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    vegetable VARCHAR(100) NOT NULL,
    top_district VARCHAR(100) NOT NULL,
    
    -- Business aggregations
    total_production_kg DECIMAL(12,2),
    total_wasted_kg DECIMAL(12,2),
    avg_wastage_percent DECIMAL(5,2),
    total_wastage_cost DECIMAL(12,2),
    
    -- Price analytics
    avg_farmer_price DECIMAL(8,2),
    avg_market_price DECIMAL(8,2),
    avg_price_gap DECIMAL(8,2),
    
    -- ML predictions (basic)
    predicted_wastage_next_week DECIMAL(5,2),
    wastage_trend VARCHAR(20),
    risk_level VARCHAR(10),
    
    -- Business recommendations
    utilization_potential_kg DECIMAL(12,2),
    potential_savings DECIMAL(12,2),
    recommendation_text TEXT,
    
    -- Metadata
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_aggregated INTEGER
);

-- Indexes for performance
CREATE INDEX idx_gold_date ON gold_vegetable_metrics(report_date);
CREATE INDEX idx_gold_vegetable ON gold_vegetable_metrics(vegetable);
CREATE INDEX idx_gold_risk ON gold_vegetable_metrics(risk_level);

COMMENT ON TABLE gold_vegetable_metrics IS 'Business metrics and predictions for vegetable waste utilization';

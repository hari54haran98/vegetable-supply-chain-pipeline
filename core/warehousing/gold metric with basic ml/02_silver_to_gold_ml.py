# SILVER TO GOLD TRANSFORMATION WITH BASIC ML
# Fixed version with proper decimal handling

import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import logging
from decimal import Decimal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def calculate_basic_trend(values):
    """Simple trend calculation for ML - fixed decimal handling"""
    if len(values) < 2:
        return "stable"
    
    # Convert to float for calculations
    recent = float(values[-1])
    previous = float(values[-2]) if len(values) >= 2 else float(values[0])
    
    if recent > previous * 1.1:
        return "increasing"
    elif recent < previous * 0.9:
        return "decreasing"
    else:
        return "stable"

def predict_next_week(current_value, trend):
    """Simple prediction based on current value and trend"""
    current_float = float(current_value)
    
    if trend == "increasing":
        return min(current_float * 1.05, 100.0)  # Cap at 100%
    elif trend == "decreasing":
        return max(current_float * 0.95, 0.0)   # Floor at 0%
    else:
        return current_float

def calculate_risk_level(wastage_percent, trend):
    """Basic risk assessment"""
    wastage_float = float(wastage_percent)
    
    if wastage_float > 40:
        return "high"
    elif wastage_float > 25:
        return "medium" if trend == "increasing" else "low"
    else:
        return "low"

def silver_to_gold_transformation():
    """Transform Silver data to Gold layer with business metrics"""
    
    logger.info("Starting Silver to Gold transformation with ML integration")
    
    try:
        conn = psycopg2.connect(
            host='warehouse-db',
            database='warehouse',
            user='airflow',
            password='airflow'
        )
        cursor = conn.cursor()
        
        # Get historical data for trend analysis
        cursor.execute('''
            SELECT report_date, vegetable, top_district, wastage_percent
            FROM silver_vegetable_prices
            ORDER BY report_date, vegetable, top_district
        ''')
        
        historical_data = cursor.fetchall()
        
        # Convert to DataFrame for basic analysis
        df = pd.DataFrame(historical_data, 
                         columns=['report_date', 'vegetable', 'top_district', 'wastage_percent'])
        
        # Group by week for aggregations
        cursor.execute('''
            INSERT INTO gold_vegetable_metrics (
                report_date, vegetable, top_district,
                total_production_kg, total_wasted_kg, avg_wastage_percent, total_wastage_cost,
                avg_farmer_price, avg_market_price, avg_price_gap,
                predicted_wastage_next_week, wastage_trend, risk_level,
                utilization_potential_kg, potential_savings, recommendation_text,
                records_aggregated
            )
            SELECT
                DATE_TRUNC('week', report_date) as report_date,
                vegetable,
                top_district,
                
                -- Business aggregations
                SUM(production_kg) as total_production_kg,
                SUM(wasted_kg) as total_wasted_kg,
                ROUND(AVG(wastage_percent), 2) as avg_wastage_percent,
                SUM(total_wastage_cost) as total_wastage_cost,
                
                -- Price analytics
                ROUND(AVG(farmer_price), 2) as avg_farmer_price,
                ROUND(AVG(market_price), 2) as avg_market_price,
                ROUND(AVG(price_gap_market_farmer), 2) as avg_price_gap,
                
                -- ML predictions (placeholder - will be calculated per record)
                0 as predicted_wastage_next_week,
                'stable' as wastage_trend,
                'low' as risk_level,
                
                -- Business potential (70% of waste can be utilized)
                SUM(wasted_kg) * 0.7 as utilization_potential_kg,
                SUM(total_wastage_cost) * 0.7 as potential_savings,
                
                -- Basic recommendations
                CASE 
                    WHEN AVG(wastage_percent) > 40 THEN 'Immediate action needed: High wastage detected'
                    WHEN AVG(wastage_percent) > 25 THEN 'Optimization opportunity: Moderate wastage'
                    ELSE 'Good performance: Low wastage levels'
                END as recommendation_text,
                
                COUNT(*) as records_aggregated
                
            FROM silver_vegetable_prices
            GROUP BY DATE_TRUNC('week', report_date), vegetable, top_district
            ORDER BY report_date, vegetable
        ''')
        
        # Now update with ML predictions based on historical trends
        cursor.execute('''
            SELECT id, vegetable, top_district, avg_wastage_percent, report_date
            FROM gold_vegetable_metrics
            ORDER BY report_date
        ''')
        
        gold_records = cursor.fetchall()
        
        # Simple ML: Update predictions based on trends
        update_count = 0
        for record in gold_records:
            record_id, vegetable, district, current_wastage, date = record
            
            # Get historical wastage for this vegetable-district combination
            veg_history = df[
                (df['vegetable'] == vegetable) & 
                (df['top_district'] == district)
            ].sort_values('report_date')
            
            if len(veg_history) > 1:  # Need at least 2 records for trend analysis
                wastage_values = veg_history['wastage_percent'].tolist()
                trend = calculate_basic_trend(wastage_values)
                prediction = predict_next_week(current_wastage, trend)
                risk = calculate_risk_level(current_wastage, trend)
                
                # Update with ML predictions
                cursor.execute('''
                    UPDATE gold_vegetable_metrics 
                    SET predicted_wastage_next_week = %s,
                        wastage_trend = %s,
                        risk_level = %s
                    WHERE id = %s
                ''', (Decimal(str(prediction)), trend, risk, record_id))
                update_count += 1
        
        # Final verification
        cursor.execute("SELECT COUNT(*) FROM gold_vegetable_metrics")
        gold_count = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT 
                COUNT(*) as total_metrics,
                ROUND(AVG(avg_wastage_percent), 2) as avg_wastage,
                COUNT(CASE WHEN risk_level = 'high' THEN 1 END) as high_risk_records,
                COUNT(CASE WHEN risk_level = 'medium' THEN 1 END) as medium_risk_records
            FROM gold_vegetable_metrics
        ''')
        
        stats = cursor.fetchone()
        
        logger.info(f"Gold transformation completed: {gold_count} business metrics created")
        logger.info(f"ML predictions applied to: {update_count} records")
        logger.info(f"Average wastage: {stats[1]}%")
        logger.info(f"High risk records: {stats[2]}")
        logger.info(f"Medium risk records: {stats[3]}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Gold transformation failed: {e}")
        raise

if __name__ == "__main__":
    silver_to_gold_transformation()

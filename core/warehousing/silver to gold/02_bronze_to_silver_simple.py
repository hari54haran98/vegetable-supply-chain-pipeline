# BRONZE TO SILVER TRANSFORMATION (SIMPLE VERSION)
# Intermediate-friendly with clear step-by-step logic

import psycopg2
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger()

def simple_silver_transformation():
    """Simple Bronze to Silver transformation - Easy to understand"""
    
    logger.info("🔄 Starting SIMPLE Bronze to Silver transformation")
    
    try:
        # Step 1: Connect to database
        conn = psycopg2.connect(
            host='warehouse-db',
            database='warehouse', 
            user='airflow',
            password='airflow'
        )
        cursor = conn.cursor()
        
        # Step 2: Check how many records we have to process
        cursor.execute("SELECT COUNT(*) FROM bronze_vegetable_prices")
        bronze_count = cursor.fetchone()[0]
        logger.info(f"📊 Found {bronze_count} records in Bronze layer")
        
        # Step 3: Simple transformation logic
        transform_sql = '''
        INSERT INTO silver_vegetable_prices (
            bronze_id, report_date, vegetable, top_district, season, quality_grade,
            production_kg, wasted_kg, wastage_percent,
            farmer_price, market_price, buyer_price,
            price_gap_market_farmer, price_gap_market_buyer, total_wastage_cost,
            is_high_wastage, is_price_anomaly, is_data_complete,
            data_quality_score
        )
        SELECT
            -- Basic field mapping
            id as bronze_id,
            report_date,
            TRIM(vegetable) as vegetable,
            TRIM(top_district) as top_district,
            TRIM(season) as season,
            TRIM(quality_grade) as quality_grade,
            
            -- Quantitative data (with basic cleaning)
            production_kg,
            wasted_kg,
            -- CALCULATE wastage_percent to avoid constraint issues
            CASE 
                WHEN production_kg > 0 THEN ROUND((wasted_kg / production_kg) * 100, 2)
                ELSE 0 
            END as wastage_percent,
            
            -- Prices
            farmer_price,
            market_price,
            buyer_price,
            
            -- Simple calculations
            market_price - farmer_price as price_gap_market_farmer,
            market_price - buyer_price as price_gap_market_buyer,
            wasted_kg * farmer_price as total_wastage_cost,
            
            -- Simple business rules
            CASE 
                WHEN production_kg > 0 THEN (wasted_kg / production_kg) * 100 > 30
                ELSE FALSE 
            END as is_high_wastage,
            
            market_price > farmer_price * 2 as is_price_anomaly,
            
            -- Basic completeness check
            (vegetable IS NOT NULL AND report_date IS NOT NULL) as is_data_complete,
            
            -- Simple quality scoring
            CASE
                WHEN vegetable IS NULL OR report_date IS NULL THEN 50
                WHEN production_kg IS NULL OR wasted_kg IS NULL THEN 75
                ELSE 100
            END as data_quality_score
            
        FROM bronze_vegetable_prices
        '''
        
        # Step 4: Execute the transformation
        cursor.execute(transform_sql)
        records_processed = cursor.rowcount
        
        # Step 5: Verify results
        cursor.execute("SELECT COUNT(*) FROM silver_vegetable_prices")
        silver_count = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT 
                COUNT(*) as total,
                AVG(data_quality_score) as avg_quality,
                COUNT(CASE WHEN is_high_wastage THEN 1 END) as high_wastage
            FROM silver_vegetable_prices
        ''')
        stats = cursor.fetchone()
        
        logger.info("✅ TRANSFORMATION SUCCESSFUL!")
        logger.info(f"📈 Processed: {records_processed} records")
        logger.info(f"🏆 Silver records: {silver_count}")
        logger.info(f"⭐ Average quality: {stats[1]:.1f}/100")
        logger.info(f"⚠️  High wastage records: {stats[2]}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}")
        logger.info("💡 Check the error message above for debugging")

# Run the transformation
if __name__ == "__main__":
    simple_silver_transformation()

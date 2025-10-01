import psycopg2
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_silver_layer():
    """Transform Bronze data to Silver layer with cleaning and validation"""
    logger.info("🔄 Starting Bronze to Silver transformation...")
    
    try:
        conn = psycopg2.connect(
            host='warehouse-db', database='warehouse',
            user='airflow', password='airflow'
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Step 1: Count records in Bronze for progress tracking
        cursor.execute("SELECT COUNT(*) FROM bronze_vegetable_prices")
        total_bronze = cursor.fetchone()[0]
        logger.info(f"📊 Bronze records to process: {total_bronze}")
        
        # Step 2: Transform and clean data from Bronze to Silver
        transformation_sql = '''
        INSERT INTO silver_vegetable_prices (
            bronze_id, report_date, vegetable, top_district, season, quality_grade,
            production_kg, wasted_kg, wastage_percent,
            farmer_price, market_price, buyer_price,
            price_gap_market_farmer, price_gap_market_buyer, total_wastage_cost,
            is_high_wastage, is_price_anomaly, is_data_complete,
            data_quality_score
        )
        SELECT 
            -- Core identifiers
            b.id as bronze_id,
            
            -- Cleaned core fields (with validation)
            CAST(b.report_date AS DATE) as report_date,
            TRIM(b.vegetable) as vegetable,
            TRIM(b.top_district) as top_district,
            TRIM(b.season) as season,
            TRIM(b.quality_grade) as quality_grade,
            
            -- Validated quantitative fields
            CASE 
                WHEN b.production_kg > 0 THEN b.production_kg
                ELSE NULL 
            END as production_kg,
            
            CASE 
                WHEN b.wasted_kg >= 0 THEN b.wasted_kg
                ELSE NULL 
            END as wasted_kg,
            
            CASE 
                WHEN b.wastage_percent BETWEEN 0 AND 100 THEN b.wastage_percent
                ELSE NULL 
            END as wastage_percent,
            
            -- Price validation
            CASE 
                WHEN b.farmer_price >= 0 THEN b.farmer_price
                ELSE NULL 
            END as farmer_price,
            
            CASE 
                WHEN b.market_price >= 0 THEN b.market_price
                ELSE NULL 
            END as market_price,
            
            CASE 
                WHEN b.buyer_price >= 0 THEN b.buyer_price
                ELSE NULL 
            END as buyer_price,
            
            -- Calculated fields
            b.market_price - b.farmer_price as price_gap_market_farmer,
            b.market_price - b.buyer_price as price_gap_market_buyer,
            b.wasted_kg * b.farmer_price as total_wastage_cost,
            
            -- Quality flags
            b.wastage_percent > 30 as is_high_wastage,
            (b.market_price > b.farmer_price * 2) as is_price_anomaly,
            
            -- Data completeness check
            (b.vegetable IS NOT NULL AND b.report_date IS NOT NULL AND 
             b.top_district IS NOT NULL AND b.production_kg IS NOT NULL) as is_data_complete,
            
            -- Data quality score (0-100)
            CASE 
                WHEN b.vegetable IS NULL OR b.report_date IS NULL OR b.top_district IS NULL THEN 0
                WHEN b.production_kg IS NULL OR b.wasted_kg IS NULL THEN 50
                WHEN b.wastage_percent NOT BETWEEN 0 AND 100 THEN 75
                ELSE 100 
            END as data_quality_score
            
        FROM bronze_vegetable_prices b
        WHERE NOT EXISTS (
            SELECT 1 FROM silver_vegetable_prices s 
            WHERE s.bronze_id = b.id
        )
        '''
        
        cursor.execute(transformation_sql)
        records_processed = cursor.rowcount
        
        # Step 3: Log transformation results
        cursor.execute('''
            SELECT 
                COUNT(*) as total_silver,
                COUNT(CASE WHEN is_data_complete THEN 1 END) as complete_records,
                COUNT(CASE WHEN is_high_wastage THEN 1 END) as high_wastage_records,
                AVG(data_quality_score) as avg_quality_score
            FROM silver_vegetable_prices
        ''')
        
        stats = cursor.fetchone()
        
        logger.info(f"✅ Silver transformation completed!")
        logger.info(f"📊 Records processed: {records_processed}")
        logger.info(f"📈 Total Silver records: {stats[0]}")
        logger.info(f"✅ Complete records: {stats[1]}")
        logger.info(f"⚠️  High wastage records: {stats[2]}")
        logger.info(f"🏆 Average quality score: {stats[3]:.1f}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Silver transformation failed: {e}")
        raise

if __name__ == "__main__":
    create_silver_layer()

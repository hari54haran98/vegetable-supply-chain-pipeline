import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def safe_float(value, default=0.0):
    if value is None: return default
    try: return float(value)
    except: return default

def main():
    logger.info("🚀 LOADING ALL 600 RECORDS FROM BEGINNING...")
    
    # Consumer that starts from VERY beginning
    consumer = KafkaConsumer(
        'vegetable_data',
        bootstrap_servers='redpanda:9092',
        group_id='complete_600_loader_$(Get-Date -Format "HHmmss")',  # New group each time
        auto_offset_reset='earliest',  # Start from very beginning
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=False
    )
    
    conn = psycopg2.connect(
        host='warehouse-db', database='warehouse', 
        user='airflow', password='airflow'
    )
    conn.autocommit = True
    
    processed = 0
    target = 600
    
    # First, clear any existing offsets to start fresh
    consumer.seek_to_beginning()
    
    logger.info("📥 Reading ALL messages from very beginning...")
    
    for message in consumer:
        record = message.value
        
        cursor = conn.cursor()
        insert_sql = '''
        INSERT INTO bronze_vegetable_prices (
            raw, report_date, vegetable, quality_grade, season, top_district,
            production_kg, wasted_kg, wastage_percent,
            market_price, buyer_price, farmer_price, ingested_at_utc
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        
        values = (
            json.dumps(record),
            record.get('Report_Date'),
            record.get('Vegetable'),
            record.get('Quality_Grade'),
            record.get('Season'),
            record.get('Top_District'),
            safe_float(record.get('Production_KG')),
            safe_float(record.get('Wasted_KG')),
            safe_float(record.get('Wastage_Percent')),
            safe_float(record.get('Market_Price')),
            safe_float(record.get('Buyer_Price')),
            safe_float(record.get('Farmer_Price')),
            datetime.now()
        )
        
        cursor.execute(insert_sql, values)
        cursor.close()
        processed += 1
        
        if processed % 100 == 0:
            logger.info(f"📊 Progress: {processed}/600 records")
        
        if processed >= target:
            break
    
    consumer.close()
    conn.close()
    logger.info(f"🎉 COMPLETED! Loaded {processed} records")

if __name__ == "__main__":
    main()

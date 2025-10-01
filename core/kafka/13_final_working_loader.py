print("🚀 FIXED BRONZE LOADER - Including all required columns")
print("Loading all 600 records from CSV to PostgreSQL...")

import pandas as pd
import psycopg2
from datetime import datetime
import json

try:
    # Load CSV
    df = pd.read_csv('/home/jovyan/work/TN_wastage_data.csv')
    print(f"✅ CSV loaded: {len(df)} records")
    
    # Connect to database
    conn = psycopg2.connect(
        host='warehouse-db',
        database='warehouse', 
        user='airflow',
        password='airflow'
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    inserted = 0
    
    for index, row in df.iterrows():
        # Create a complete record dictionary for the raw JSON column
        record_dict = {}
        for col in df.columns:
            value = row[col]
            # Convert numpy types to Python native types
            if hasattr(value, 'item'):
                value = value.item()
            record_dict[col] = value
        
        # Insert with ALL required columns including the raw JSON
        sql = """
        INSERT INTO bronze_vegetable_prices (
            raw, report_date, vegetable, quality_grade, season, top_district,
            production_kg, wasted_kg, wastage_percent, market_price, buyer_price, 
            farmer_price, ingested_at_utc
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (
            json.dumps(record_dict),  # raw JSON column - REQUIRED
            row['Report_Date'],
            row['Vegetable'],
            row['Quality_Grade'],
            row['Season'],
            row['Top_District'],
            float(row['Production_KG']),
            float(row['Wasted_KG']),
            float(row['Wastage_Percent']),
            float(row['Market_Price']),
            float(row['Buyer_Price']),
            float(row['Farmer_Price']),
            datetime.now()
        )
        
        cursor.execute(sql, values)
        inserted += 1
        
        if inserted % 100 == 0:
            print(f"📦 Inserted {inserted} records...")
    
    cursor.close()
    conn.close()
    
    print(f"🎉 SUCCESS! Inserted {inserted} records into bronze_vegetable_prices")
    
except Exception as e:
    print(f"❌ ERROR: {e}")
    import traceback
    traceback.print_exc()

import psycopg2
import pandas as pd
from datetime import datetime

def check_bronze_data_quality():
    print("🔍 BRONZE LAYER DATA QUALITY REPORT")
    print("=" * 50)
    
    conn = psycopg2.connect(
        host='warehouse-db', database='warehouse', 
        user='airflow', password='airflow'
    )
    
    # Basic counts and metrics
    queries = {
        "Total Records": "SELECT COUNT(*) FROM bronze_vegetable_prices",
        "Unique Vegetables": "SELECT COUNT(DISTINCT vegetable) FROM bronze_vegetable_prices",
        "Unique Districts": "SELECT COUNT(DISTINCT top_district) FROM bronze_vegetable_prices",
        "Date Range": "SELECT MIN(report_date), MAX(report_date) FROM bronze_vegetable_prices",
        "Avg Wastage %": "SELECT ROUND(AVG(wastage_percent), 2) FROM bronze_vegetable_prices",
        "Data Completeness": '''
            SELECT 
                ROUND(100.0 * COUNT(vegetable) / COUNT(*), 2) as vegetable_complete,
                ROUND(100.0 * COUNT(wastage_percent) / COUNT(*), 2) as wastage_complete,
                ROUND(100.0 * COUNT(market_price) / COUNT(*), 2) as price_complete
            FROM bronze_vegetable_prices
        '''
    }
    
    for metric, query in queries.items():
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        print(f"✅ {metric}: {result[0]}")
        cursor.close()
    
    # Vegetable distribution
    print("\n🥦 VEGETABLE DISTRIBUTION:")
    cursor = conn.cursor()
    cursor.execute('''
        SELECT vegetable, COUNT(*) as count, 
               ROUND(AVG(wastage_percent), 2) as avg_wastage,
               ROUND(AVG(market_price), 2) as avg_price
        FROM bronze_vegetable_prices 
        GROUP BY vegetable 
        ORDER BY count DESC
    ''')
    
    for row in cursor.fetchall():
        print(f"   {row[0]}: {row[1]} records, {row[2]}% wastage, ₹{row[3]} avg price")
    
    cursor.close()
    
    # District analysis
    print("\n🏙️ TOP DISTRICTS BY WASTAGE:")
    cursor = conn.cursor()
    cursor.execute('''
        SELECT top_district, COUNT(*) as count,
               ROUND(AVG(wastage_percent), 2) as avg_wastage,
               ROUND(SUM(wasted_kg), 2) as total_wastage_kg
        FROM bronze_vegetable_prices 
        GROUP BY top_district 
        ORDER BY total_wastage_kg DESC 
        LIMIT 5
    ''')
    
    for row in cursor.fetchall():
        print(f"   {row[0]}: {row[1]} records, {row[2]}% wastage, {row[3]}kg total")
    
    cursor.close()
    conn.close()
    
    print("=" * 50)
    print("📊 DATA QUALITY CHECK COMPLETED")
    print(f"⏰ Generated at: {datetime.now()}")

if __name__ == "__main__":
    check_bronze_data_quality()

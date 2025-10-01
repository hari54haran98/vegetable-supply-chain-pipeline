print("🔍 FINAL BRONZE LAYER VERIFICATION")
print("=" * 50)

import psycopg2

try:
    conn = psycopg2.connect(
        host='warehouse-db',
        database='warehouse', 
        user='airflow',
        password='airflow'
    )
    cursor = conn.cursor()
    
    # Get final counts
    cursor.execute("SELECT COUNT(*) FROM bronze_vegetable_prices")
    total_records = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT vegetable) FROM bronze_vegetable_prices")
    unique_vegetables = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT top_district) FROM bronze_vegetable_prices")
    unique_districts = cursor.fetchone()[0]
    
    cursor.execute("SELECT ROUND(AVG(wastage_percent), 2) FROM bronze_vegetable_prices")
    avg_wastage = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    print(f"✅ Total records: {total_records}/600")
    print(f"✅ Unique vegetables: {unique_vegetables}")
    print(f"✅ Unique districts: {unique_districts}")
    print(f"✅ Average wastage: {avg_wastage}%")
    
    if total_records == 600:
        print("🎉 BRONZE LAYER: 100% COMPLETED SUCCESSFULLY!")
    else:
        print(f"⚠️  Bronze layer: {total_records}/600 records")
    
except Exception as e:
    print(f"❌ Verification error: {e}")

print("=" * 50)

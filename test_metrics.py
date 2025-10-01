import pytest
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from pipeline_metrics import get_db_connection

class TestBusinessMetrics:
    
    def test_database_connection(self):
        """Test that we can connect to the database"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1
            cursor.close()
            conn.close()
            print("✅ Database connection test passed")
        except Exception as e:
            pytest.fail(f"Database connection failed: {e}")
    
    def test_wastage_percentage_range(self):
        """Test that wastage percentage is always between 0-100"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT wastage_percent FROM silver_vegetable_prices WHERE wastage_percent IS NOT NULL LIMIT 10")
            results = cursor.fetchall()
            
            for wastage, in results:
                assert 0 <= wastage <= 100, f"Invalid wastage percentage: {wastage}"
            
            cursor.close()
            conn.close()
            print("✅ Wastage percentage range test passed")
        except Exception as e:
            pytest.fail(f"Wastage percentage test failed: {e}")
    
    def test_bronze_data_exists(self):
        """Test that bronze layer has data"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM bronze_vegetable_prices")
            count = cursor.fetchone()[0]
            
            assert count > 0, "Bronze layer should have data"
            print(f"✅ Bronze data test passed - {count} rows found")
            
            cursor.close()
            conn.close()
        except Exception as e:
            pytest.fail(f"Bronze data test failed: {e}")

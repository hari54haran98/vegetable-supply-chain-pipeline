# bronze_to_silver_dag.py - FIXED VERSION
# Airflow DAG for Bronze to Silver transformation

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'bronze_to_silver_transformation',
    default_args=default_args,
    description='Transform Bronze layer data to Silver layer',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['vegetable-pipeline', 'silver-layer']
)

def bronze_to_silver_transformation():
    """Python function to execute Bronze to Silver transformation"""
    try:
        # Direct connection (simpler approach)
        conn = psycopg2.connect(
            host='warehouse-db',
            database='warehouse',
            user='airflow',
            password='airflow'
        )
        cursor = conn.cursor()
        
        # Simple transformation
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
            id as bronze_id,
            report_date,
            TRIM(vegetable) as vegetable,
            TRIM(top_district) as top_district,
            TRIM(season) as season,
            TRIM(quality_grade) as quality_grade,
            production_kg,
            wasted_kg,
            CASE 
                WHEN production_kg > 0 THEN ROUND((wasted_kg / production_kg) * 100, 2)
                ELSE 0 
            END as wastage_percent,
            farmer_price,
            market_price,
            buyer_price,
            market_price - farmer_price as price_gap_market_farmer,
            market_price - buyer_price as price_gap_market_buyer,
            wasted_kg * farmer_price as total_wastage_cost,
            CASE 
                WHEN production_kg > 0 THEN (wasted_kg / production_kg) * 100 > 30
                ELSE FALSE 
            END as is_high_wastage,
            market_price > farmer_price * 2 as is_price_anomaly,
            (vegetable IS NOT NULL AND report_date IS NOT NULL) as is_data_complete,
            CASE
                WHEN vegetable IS NULL OR report_date IS NULL THEN 50
                WHEN production_kg IS NULL OR wasted_kg IS NULL THEN 75
                ELSE 100
            END as data_quality_score
        FROM bronze_vegetable_prices
        WHERE NOT EXISTS (
            SELECT 1 FROM silver_vegetable_prices s 
            WHERE s.bronze_id = bronze_vegetable_prices.id
        )
        '''
        
        cursor.execute(transformation_sql)
        records_processed = cursor.rowcount
        
        logging.info(f"Bronze to Silver transformation completed: {records_processed} records processed")
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Bronze to Silver transformation failed: {e}")
        raise

# Define task
bronze_to_silver_task = PythonOperator(
    task_id='execute_bronze_to_silver',
    python_callable=bronze_to_silver_transformation,
    dag=dag
)

# silver_to_gold_dag.py - FIXED FOR AIRFLOW 2.9.1
# Airflow DAG for Silver to Gold transformation

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    'silver_to_gold_transformation',
    default_args=default_args,
    description='Transform Silver layer data to Gold layer with ML',
    schedule_interval=timedelta(hours=2),
    catchup=False,
    tags=['vegetable-pipeline', 'gold-layer', 'ml']
)

def silver_to_gold_transformation():
    """Python function to execute Silver to Gold transformation"""
    import psycopg2
    import pandas as pd
    
    try:
        # Direct connection - simpler and more reliable
        conn = psycopg2.connect(
            host='warehouse-db',
            database='warehouse',
            user='airflow',
            password='airflow'
        )
        cursor = conn.cursor()
        
        logging.info("Starting Silver to Gold transformation...")
        
        # Clear existing gold data for fresh transformation
        cursor.execute('TRUNCATE TABLE gold_vegetable_metrics;')
        
        # Perform the aggregation (from our working Gold layer)
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
                SUM(production_kg) as total_production_kg,
                SUM(wasted_kg) as total_wasted_kg,
                ROUND(AVG(wastage_percent), 2) as avg_wastage_percent,
                SUM(total_wastage_cost) as total_wastage_cost,
                ROUND(AVG(farmer_price), 2) as avg_farmer_price,
                ROUND(AVG(market_price), 2) as avg_market_price,
                ROUND(AVG(price_gap_market_farmer), 2) as avg_price_gap,
                0 as predicted_wastage_next_week,
                'stable' as wastage_trend,
                CASE 
                    WHEN AVG(wastage_percent) > 40 THEN 'high'
                    WHEN AVG(wastage_percent) > 25 THEN 'medium' 
                    ELSE 'low'
                END as risk_level,
                SUM(wasted_kg) * 0.7 as utilization_potential_kg,
                SUM(total_wastage_cost) * 0.7 as potential_savings,
                CASE 
                    WHEN AVG(wastage_percent) > 40 THEN 'Immediate action needed: High wastage detected'
                    WHEN AVG(wastage_percent) > 25 THEN 'Optimization opportunity: Moderate wastage'
                    ELSE 'Good performance: Low wastage levels'
                END as recommendation_text,
                COUNT(*) as records_aggregated
            FROM silver_vegetable_prices
            GROUP BY DATE_TRUNC('week', report_date), vegetable, top_district
        ''')
        
        records_processed = cursor.rowcount
        logging.info(f"Silver to Gold transformation completed: {records_processed} records processed")
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Silver to Gold transformation failed: {e}")
        raise

# Define task
silver_to_gold_task = PythonOperator(
    task_id='execute_silver_to_gold',
    python_callable=silver_to_gold_transformation,
    dag=dag
)

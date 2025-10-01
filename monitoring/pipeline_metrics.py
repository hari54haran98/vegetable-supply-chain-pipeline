#!/usr/bin/env python3
"""
Custom Data Pipeline Metrics Collector
Monitors business KPIs and data quality metrics
"""

from prometheus_client import start_http_server, Gauge, Counter, Summary
import psycopg2
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
# Infrastructure Metrics
postgres_connections = Gauge('postgres_connections', 'Active PostgreSQL connections')
postgres_query_duration = Summary('postgres_query_duration_seconds', 'PostgreSQL query duration')

# Data Pipeline Metrics
kafka_messages_ingested = Counter('kafka_messages_ingested_total', 'Total messages ingested from Kafka')
bronze_row_count = Gauge('bronze_layer_row_count', 'Number of rows in bronze layer')
silver_row_count = Gauge('silver_layer_row_count', 'Number of rows in silver layer')
gold_row_count = Gauge('gold_layer_row_count', 'Number of rows in gold layer')

# Business KPIs
total_wastage_percentage = Gauge('total_wastage_percentage', 'Total wastage percentage')
top_vegetable_wastage = Gauge('top_vegetable_wastage', 'Wastage percentage for top vegetable', ['vegetable'])
avoidable_loss_cost = Gauge('avoidable_loss_cost', 'Estimated avoidable loss cost in INR')

def get_db_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(
        host="host.docker.internal",
        port=5433,
        database="warehouse",
        user="airflow",
        password="airflow"
    )

def monitor_infrastructure():
    """Monitor infrastructure metrics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get active connections
        cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE datname = 'warehouse';")
        active_connections = cursor.fetchone()[0]
        postgres_connections.set(active_connections)

        cursor.close()
        conn.close()
        logger.info(f"Infrastructure metrics updated - Connections: {active_connections}")

    except Exception as e:
        logger.error(f"Error monitoring infrastructure: {e}")

def monitor_data_pipeline():
    """Monitor data pipeline metrics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Bronze layer metrics
        cursor.execute("SELECT COUNT(*) FROM bronze_vegetable_prices;")
        bronze_count = cursor.fetchone()[0]
        bronze_row_count.set(bronze_count)

        # Silver layer metrics
        cursor.execute("SELECT COUNT(*) FROM silver_vegetable_prices;")
        silver_count = cursor.fetchone()[0]
        silver_row_count.set(silver_count)

        # Gold layer metrics
        cursor.execute("SELECT COUNT(*) FROM gold_vegetable_metrics;")
        gold_count = cursor.fetchone()[0]
        gold_row_count.set(gold_count)

        # Business KPIs - wastage metrics - FIXED COLUMN NAMES
        cursor.execute("""
            SELECT AVG(wastage_percent),
                   MAX(wastage_percent),
                   SUM(total_wastage_cost)
            FROM silver_vegetable_prices
            WHERE wastage_percent > 0;
        """)
        result = cursor.fetchone()
        if result and result[0]:
            avg_wastage, max_wastage, total_loss = result
            total_wastage_percentage.set(avg_wastage)
            avoidable_loss_cost.set(total_loss or 0)
        else:
            total_wastage_percentage.set(0)
            avoidable_loss_cost.set(0)

        # Top vegetable wastage - FIXED COLUMN NAME
        cursor.execute("""
            SELECT vegetable, AVG(wastage_percent)
            FROM silver_vegetable_prices
            GROUP BY vegetable
            ORDER BY AVG(wastage_percent) DESC
            LIMIT 3;
        """)

        # Clear previous labels
        for vegetable in ['Tomato', 'Potato', 'Onion']:  # Common vegetables
            try:
                top_vegetable_wastage.remove(vegetable)
            except:
                pass

        # Set new values
        for vegetable, wastage in cursor.fetchall():
            top_vegetable_wastage.labels(vegetable=vegetable).set(wastage)

        cursor.close()
        conn.close()

        logger.info(f"Pipeline metrics - Bronze: {bronze_count}, Silver: {silver_count}, Gold: {gold_count}")

    except Exception as e:
        logger.error(f"Error monitoring data pipeline: {e}")

if __name__ == '__main__':
    # Start Prometheus metrics server
    start_http_server(8000)
    logger.info("Data Pipeline Metrics server started on port 8000")

    while True:
        monitor_infrastructure()
        monitor_data_pipeline()
        time.sleep(30)  # Update every 30 seconds

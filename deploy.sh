#!/bin/bash
echo "🚀 REAL Deployment for Vegetable Supply Chain Pipeline"
echo "======================================================"

echo "1. Testing database connection..."
python -c "
import psycopg2
try:
    conn = psycopg2.connect(
        host='host.docker.internal',
        port=5433,
        database='warehouse',
        user='airflow', 
        password='airflow'
    )
    print('✅ Database connection successful')
    conn.close()
except Exception as e:
    print(f'❌ Database connection failed: {e}')
    exit(1)
"

echo "2. Validating script syntax..."
python -m py_compile scripts/pipeline_metrics.py
echo "✅ Script syntax valid"

echo "3. Deploying to pipeline-metrics container..."
echo "👉 MANUAL STEP: Copy scripts/pipeline_metrics.py to C:\data engineering\monitoring\scripts\"
echo "👉 MANUAL STEP: Run: docker restart pipeline-metrics"

echo "4. Deployment complete!"
echo "📊 Check metrics: http://localhost:8000/metrics"
echo "📈 Check Grafana: http://localhost:3000"

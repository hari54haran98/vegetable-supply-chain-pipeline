# vegetable_pipeline_master_dag.py - FIXED VERSION
# Master DAG orchestrating the entire vegetable data pipeline

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    'vegetable_pipeline_master',
    default_args=default_args,
    description='Master DAG for end-to-end vegetable data pipeline',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['vegetable-pipeline', 'master']
)

# Define tasks
start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

trigger_bronze_to_silver = TriggerDagRunOperator(
    task_id='trigger_bronze_to_silver',
    trigger_dag_id='bronze_to_silver_transformation',
    dag=dag
)

trigger_silver_to_gold = TriggerDagRunOperator(
    task_id='trigger_silver_to_gold',
    trigger_dag_id='silver_to_gold_transformation',
    dag=dag
)

pipeline_complete = DummyOperator(
    task_id='pipeline_complete',
    dag=dag
)

# Define workflow
start_pipeline >> trigger_bronze_to_silver >> trigger_silver_to_gold >> pipeline_complete

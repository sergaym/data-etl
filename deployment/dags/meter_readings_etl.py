"""
Airflow DAG for meter readings ETL pipeline.

Flow:
1. Task 1: Extract & Store Raw (8:30 AM)
   - Extract from JSON files (available by 8am)
   - Extract from SQLite DB (updated at midnight)
   - Store in PostgreSQL raw schema

2. Task 2: Transform & Load Analytics (9:00 AM)
   - Read from PostgreSQL raw schema
   - Transform data
   - Load to analytics schema
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.pipelines.etl import extract_and_store_raw, transform_and_load_analytics
from src.loading import PostgresWriter

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

def run_extract_and_store(**context):
    """Task 1: Extract from sources and store raw data"""
    writer = PostgresWriter()
    latest_ts = writer.get_latest_reading_timestamp()
    return extract_and_store_raw(start_date=latest_ts)

def run_transform_and_load(**context):
    """Task 2: Transform raw data and load analytics"""
    execution_date = context['execution_date']
    reference_date = execution_date.strftime('%Y-%m-%d')
    return transform_and_load_analytics(reference_date)

# Create DAG
with DAG(
    'meter_readings_etl',
    default_args=default_args,
    description='Two-phase ETL pipeline for meter readings data',
    schedule_interval='30 8 * * *',  # Start at 8:30 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'meter_readings'],
    max_active_runs=1
) as dag:
    
    # Wait for database update to complete
    wait_for_db = TimeDeltaSensor(
        task_id='wait_for_db_update',
        delta=timedelta(minutes=30),  # Wait 30 mins after midnight
        mode='poke',
        poke_interval=300,  # Check every 5 minutes
        dag=dag
    )
    
    # Task 1: Extract and store raw data
    extract_raw = PythonOperator(
        task_id='extract_and_store_raw',
        python_callable=run_extract_and_store,
        provide_context=True,
        dag=dag
    )
    
    # Task 2: Transform and load analytics
    load_analytics = PythonOperator(
        task_id='transform_and_load_analytics',
        python_callable=run_transform_and_load,
        provide_context=True,
        dag=dag
    )
    
    # Set task dependencies
    wait_for_db >> extract_raw >> load_analytics 
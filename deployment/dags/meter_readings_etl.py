"""
Airflow DAG for meter readings ETL pipeline.
Runs daily after both data sources are updated:
- Readings files (available by 8am)
- Database updates (at midnight)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.pipelines.etl import run_etl

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

# Create DAG
dag = DAG(
    'meter_readings_etl',
    default_args=default_args,
    description='Daily ETL pipeline for meter readings data',
    schedule_interval='0 9 * * *',  # Run at 9am daily (after both sources are updated)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'meter_readings']
)

def run_etl_task(**context):
    """Wrapper function to run ETL with current date"""
    execution_date = context['execution_date']
    reference_date = execution_date.strftime('%Y-%m-%d')
    run_etl(reference_date=reference_date)

# Define task
etl_task = PythonOperator(
    task_id='run_etl_pipeline',
    python_callable=run_etl_task,
    provide_context=True,
    dag=dag
)

# Set task dependencies (single task in this case)
etl_task 
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def create_dataframe():
    data = {'Name': ['John', 'Anna', 'Peter', 'Linda'],
            'Age': [28, 24, 35, 32]}
    df = pd.DataFrame(data)
    df.to_csv('/tmp/data.csv', index=False)
    print("DataFrame created and saved to /tmp/data.csv")

def process_dataframe():
    df = pd.read_csv('/tmp/data.csv')
    df['Age'] = df['Age'] + 1  # Example processing: increment age by 1
    df.to_csv('/tmp/processed_data.csv', index=False)
    print("DataFrame processed and saved to /tmp/processed_data.csv")

def print_dataframe():
    df = pd.read_csv('/tmp/processed_data.csv')
    print("Processed DataFrame:")
    print(df)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sophisticated_dag',
    default_args=default_args,
    description='A more sophisticated DAG with DataFrame operations',
    schedule_interval=timedelta(days=1),
)

create_task = PythonOperator(
    task_id='create_dataframe',
    python_callable=create_dataframe,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_dataframe',
    python_callable=process_dataframe,
    dag=dag,
)

print_task = PythonOperator(
    task_id='print_dataframe',
    python_callable=print_dataframe,
    dag=dag,
)

create_task >> process_task >> print_task
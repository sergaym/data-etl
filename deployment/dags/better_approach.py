from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.pipelines.etl import (
    load_json_readings,
    get_data_summary,
    DatabaseLoader,
    PostgresWriter,
    DataTransformer
)
from src.loading.postgres_reader import PostgresReader

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 9, 15),
}

def extract_json():
    df_readings = load_json_readings()
    return df_readings

def extract_sqlite():
    db = DatabaseLoader()
    df_agreement = db.load_table('agreement')
    df_product = db.load_table('product')
    df_meterpoint = db.load_table('meterpoint')
    return df_agreement, df_product, df_meterpoint

def store_raw_data(df_readings, df_agreement, df_product, df_meterpoint):
    writer = PostgresWriter()
    writer.ensure_schema_exists(writer.raw_schema)
    writer.ensure_raw_tables_exist()
    writer.load_raw_readings(df_readings)
    reference_data = {
        'raw_agreements': df_agreement,
        'raw_products': df_product,
        'raw_meterpoints': df_meterpoint
    }
    for table_name, df in reference_data.items():
        writer.load_raw_reference_data(table_name, df)

def transform_data():
    reader = PostgresReader()
    raw_data = reader.read_raw_tables()
    transformer = DataTransformer(
        df_readings=raw_data['readings'],
        df_agreement=raw_data['agreement'],
        df_product=raw_data['product'],
        df_meterpoint=raw_data['meterpoint']
    )
    df_active_agreements = transformer.get_active_agreements('2021-01-01')
    df_halfhourly = transformer.get_halfhourly_consumption()
    df_product_daily = transformer.get_daily_product_consumption()
    return df_active_agreements, df_halfhourly, df_product_daily

def load_analytics(df_active_agreements, df_halfhourly, df_product_daily):
    writer = PostgresWriter()
    writer.ensure_schema_exists(writer.analytics_schema)
    writer.write_active_agreements(df_active_agreements, '2021-01-01')
    writer.write_halfhourly_consumption(df_halfhourly)
    writer.write_daily_product_consumption(df_product_daily)

with DAG(
    dag_id="meter_readings_etl_optimized",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    extract_json_task = PythonOperator(
        task_id="extract_json",
        python_callable=extract_json
    )

    extract_sqlite_task = PythonOperator(
        task_id="extract_sqlite",
        python_callable=extract_sqlite
    )

    store_raw_data_task = PythonOperator(
        task_id="store_raw_data",
        python_callable=store_raw_data,
        op_args=[
            "{{ task_instance.xcom_pull(task_ids='extract_json') }}",
            "{{ task_instance.xcom_pull(task_ids='extract_sqlite')[0] }}",
            "{{ task_instance.xcom_pull(task_ids='extract_sqlite')[1] }}",
            "{{ task_instance.xcom_pull(task_ids='extract_sqlite')[2] }}"
        ]
    )

    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load_analytics_task = PythonOperator(
        task_id="load_analytics",
        python_callable=load_analytics,
        op_args=[
            "{{ task_instance.xcom_pull(task_ids='transform_data')[0] }}",
            "{{ task_instance.xcom_pull(task_ids='transform_data')[1] }}",
            "{{ task_instance.xcom_pull(task_ids='transform_data')[2] }}"
        ]
    )

    end = DummyOperator(task_id="end")

    start >> [extract_json_task, extract_sqlite_task] >> store_raw_data_task
    store_raw_data_task >> transform_data_task >> load_analytics_task >> end

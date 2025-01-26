"""
ETL pipeline for processing meter readings data.

Flow:
Task 1: Extract & Store Raw
1. Extract data from JSON files and SQLite
2. Store raw data in PostgreSQL (incremental)

Task 2: Transform & Load Analytics
1. Read from PostgreSQL raw tables
2. Transform data
3. Store transformed data in analytics tables
"""

import time
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Tuple
import pandas as pd

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.extraction import load_json_readings, get_data_summary, DatabaseLoader
from src.transformation import DataTransformer
from src.loading import PostgresWriter
from src.utils.logger import setup_logger
from src.loading.postgres_reader import PostgresReader

logger = setup_logger("etl_pipeline")

def extract_and_store_raw(start_date: Optional[datetime] = None) -> bool:
    """
    Task 1: Extract from source systems and store in raw PostgreSQL tables.
    
    Args:
        start_date: Optional timestamp to filter readings after this date
        
    Returns:
        bool: True if successful
    """
    try:
        total_start_time = time.time()
        logger.info("Starting raw data extraction and storage...")
        
        # Initialize writer
        writer = PostgresWriter()
        
        # Extract JSON readings with optional date filter
        df_readings = load_json_readings()
        if start_date:
            df_readings = df_readings[df_readings['interval_start'] > start_date]
            logger.info(f"Filtered readings after {start_date}")
        
        readings_summary = get_data_summary(df_readings)
        logger.info(f"Readings Data Summary: {readings_summary}")
        
        # Extract database tables
        db = DatabaseLoader()
        df_agreement = db.load_table('agreement')
        df_product = db.load_table('product')
        df_meterpoint = db.load_table('meterpoint')
        
        # Store raw data
        writer.ensure_schema_exists(writer.raw_schema)
        writer.ensure_raw_tables_exist()
        
        # Store raw meter readings
        writer.load_raw_readings(df_readings)
        
        # Store reference data
        reference_data = {
            'raw_agreements': df_agreement,
            'raw_products': df_product,
            'raw_meterpoints': df_meterpoint
        }
        
        for table_name, df in reference_data.items():
            writer.load_raw_reference_data(table_name, df)
        
        duration = time.time() - total_start_time
        logger.info(f"Raw data pipeline completed in {duration:.2f} seconds")
        return True
        
    except Exception as e:
        logger.error(f"Raw data pipeline failed: {e}", exc_info=True)
        raise

def transform_and_load_analytics(reference_date: str) -> bool:
    """
    Task 2: Transform raw data and load analytics tables.
    
    Args:
        reference_date: Reference date for processing
        
    Returns:
        bool: True if successful
    """
    try:
        start_time = time.time()
        logger.info("Starting analytics transformation and loading...")
        
        # Initialize components
        writer = PostgresWriter()
        reader = PostgresReader(writer.engine, writer.raw_schema, writer.analytics_schema)
        
        # Read raw data
        raw_data = reader.read_raw_tables()
        
        # Transform data
        transformer = DataTransformer(
            df_readings=raw_data['readings'],
            df_agreement=raw_data['agreement'],
            df_product=raw_data['product'],
            df_meterpoint=raw_data['meterpoint']
        )
        
        df_active_agreements = transformer.get_active_agreements(reference_date)
        df_halfhourly = transformer.get_halfhourly_consumption()
        df_product_daily = transformer.get_daily_product_consumption()
        
        # Store transformed data
        writer.ensure_schema_exists(writer.analytics_schema)
        writer.write_active_agreements(df_active_agreements, reference_date)
        writer.write_halfhourly_consumption(df_halfhourly)
        writer.write_daily_product_consumption(df_product_daily)
        
        duration = time.time() - start_time
        logger.info(f"Analytics pipeline completed in {duration:.2f} seconds")
        return True
        
    except Exception as e:
        logger.error(f"Analytics pipeline failed: {e}", exc_info=True)
        raise

def run_etl(reference_date: str = '2021-01-01'):
    """Run both ETL tasks in sequence."""
    try:
        total_start_time = time.time()
        logger.info(f"Starting ETL pipeline for reference date: {reference_date}")
        
        # Get latest timestamp for incremental load
        writer = PostgresWriter()
        latest_ts = writer.get_latest_reading_timestamp()
        
        # Task 1: Extract and Store Raw Data
        extract_and_store_raw(start_date=latest_ts)
        
        # Task 2: Transform and Load Analytics
        transform_and_load_analytics(reference_date)
        
        duration = time.time() - total_start_time
        logger.info(f"ETL pipeline completed successfully in {duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the ETL pipeline.")
    parser.add_argument(
        "--reference_date",
        type=str,
        default="2021-01-01",
        help="Reference date for processing agreements (YYYY-MM-DD)"
    )
    args = parser.parse_args()
    
    run_etl(args.reference_date)

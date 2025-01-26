"""
ETL pipeline for processing meter readings data.

Flow:
1. Extract data from sources
2. Store raw data in PostgreSQL (incremental)
3. Transform data
4. Store transformed data
"""

import time
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional
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

def extract_data(start_date: Optional[datetime] = None):
    """
    Extract data from JSON files and SQLite database.
    
    Args:
        start_date: Optional timestamp to filter readings after this date
    """
    try:
        start_time = time.time()
        logger.info("Starting data extraction...")
        
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
        
        logger.info(f"Data extraction completed in {time.time() - start_time:.2f} seconds")
        return df_readings, df_agreement, df_product, df_meterpoint
    
    except Exception as e:
        logger.error(f"Data extraction failed: {e}", exc_info=True)
        raise

def store_raw_data(writer: PostgresWriter, df_readings, df_agreement, df_product, df_meterpoint):
    """Store raw data in PostgreSQL before transformation."""
    try:
        total_start_time = time.time()
        logger.info("Starting raw data storage...")
        
        # Step 2.1: Ensure schemas and tables exist
        writer.ensure_schema_exists(writer.raw_schema)
        writer.ensure_raw_tables_exist()
        
        # Step 2.2: Store raw meter readings (largest table) with efficient bulk loading
        readings_start = time.time()
        writer.load_raw_readings(df_readings)
        logger.info(f"Raw readings stored in {time.time() - readings_start:.2f} seconds")
        
        # Step 2.3: Store reference data tables (smaller tables)
        ref_start = time.time()
        reference_data = {
            'raw_agreements': df_agreement,
            'raw_products': df_product,
            'raw_meterpoints': df_meterpoint
        }
        
        for table_name, df in reference_data.items():
            writer.load_raw_reference_data(table_name, df)
            
        logger.info(f"Reference data stored in {time.time() - ref_start:.2f} seconds")
        logger.info(f"Total raw data storage completed in {time.time() - total_start_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Raw data storage failed: {e}", exc_info=True)
        raise

def transform_data(writer: PostgresWriter, reference_date: str):
    """Transform data using DataTransformer module."""
    try:
        start_time = time.time()
        logger.info("Starting data transformation...")
        
        # Read raw data using PostgresReader from the same database connection
        reader = PostgresReader(writer.engine, writer.raw_schema, writer.analytics_schema)
        raw_data = reader.read_raw_tables()
        
        # Use DataTransformer for transformations
        transformer = DataTransformer(
            df_readings=raw_data['readings'],
            df_agreement=raw_data['agreement'],
            df_product=raw_data['product'],
            df_meterpoint=raw_data['meterpoint']
        )
        
        df_active_agreements = transformer.get_active_agreements(reference_date)
        df_halfhourly = transformer.get_halfhourly_consumption()
        df_product_daily = transformer.get_daily_product_consumption()
        
        logger.info(f"Data transformation completed in {time.time() - start_time:.2f} seconds")
        return df_active_agreements, df_halfhourly, df_product_daily
    
    except Exception as e:
        logger.error(f"Data transformation failed: {e}", exc_info=True)
        raise

def store_transformed_data(writer: PostgresWriter, df_active_agreements, df_halfhourly, df_product_daily, reference_date):
    """Store transformed data in PostgreSQL analytics schema."""
    try:
        start_time = time.time()
        logger.info("Starting transformed data storage...")
        
        # Ensure analytics schema exists
        writer.ensure_schema_exists(writer.analytics_schema)
        
        # Store transformed data
        writer.write_active_agreements(df_active_agreements, reference_date)
        writer.write_halfhourly_consumption(df_halfhourly)
        writer.write_daily_product_consumption(df_product_daily)
        
        logger.info(f"Transformed data storage completed in {time.time() - start_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Transformed data storage failed: {e}", exc_info=True)
        raise

def run_etl(reference_date: str = '2022-06-15'):
    """Run the complete ETL pipeline with raw data storage."""
    try:
        total_start_time = time.time()
        logger.info(f"Starting ETL pipeline for reference date: {reference_date}")
        
        # Initialize PostgreSQL writer
        writer = PostgresWriter()
        
        # Step 1: Extract Data (only new readings)
        latest_ts = writer.get_latest_reading_timestamp()
        df_readings, df_agreement, df_product, df_meterpoint = extract_data(start_date=latest_ts)
        
        # Step 2: Store Raw Data
        store_raw_data(writer, df_readings, df_agreement, df_product, df_meterpoint)

        # Step 3: Transform Data (using raw tables)
        df_active_agreements, df_halfhourly, df_product_daily = transform_data(
            writer, reference_date
        )

        # Step 4: Store Transformed Data
        store_transformed_data(
            writer, df_active_agreements, df_halfhourly, df_product_daily, reference_date
        )
        
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
        default="2022-06-15",
        help="Reference date for processing agreements (YYYY-MM-DD)"
    )
    args = parser.parse_args()
    
    run_etl(args.reference_date)

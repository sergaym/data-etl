"""
ETL pipeline for processing meter readings and loading to PostgreSQL.
"""

import time
from typing import Dict, Any

from src.extraction import (
    load_json_readings,
    get_data_summary,
    DatabaseLoader,
    DEFAULT_DB_PATH,
    DEFAULT_READINGS_PATH
)
from src.transformation import DataTransformer
from src.loading import PostgresWriter
from src.utils.logger import setup_logger

logger = setup_logger("etl_pipeline")

def extract_data() -> Dict[str, Any]:
    """Extract data from JSON files and SQLite database."""
    logger.info("Starting data extraction...")
    
    # Process JSON files
    logger.info("Loading JSON readings...")
    df_readings = load_json_readings(DEFAULT_READINGS_PATH)
    readings_summary = get_data_summary(df_readings)
    logger.info(
        f"Loaded {readings_summary['total_readings']} readings from "
        f"{readings_summary['unique_meterpoints']} meterpoints"
    )
    
    # Process SQLite database
    logger.info("Loading database tables...")
    db = DatabaseLoader(DEFAULT_DB_PATH)
    df_agreement = db.load_table('agreement')
    df_product = db.load_table('product')
    df_meterpoint = db.load_table('meterpoint')
    
    logger.info(
        f"Loaded tables - Agreements: {len(df_agreement)}, "
        f"Products: {len(df_product)}, "
        f"Meterpoints: {len(df_meterpoint)}"
    )
    
    return {
        'readings': df_readings,
        'agreement': df_agreement,
        'product': df_product,
        'meterpoint': df_meterpoint,
        'summary': readings_summary
    }

def transform_data(data: Dict[str, Any], reference_date: str) -> Dict[str, Any]:
    """Transform extracted data into analytics tables."""
    logger.info("Starting data transformation...")
    
    transformer = DataTransformer(
        df_readings=data['readings'],
        df_agreement=data['agreement'],
        df_product=data['product'],
        df_meterpoint=data['meterpoint']
    )
    
    # Generate analytics tables
    df_active_agreements = transformer.get_active_agreements(reference_date)
    df_halfhourly = transformer.get_halfhourly_consumption()
    df_product_daily = transformer.get_daily_product_consumption()
    
    logger.info(
        f"Generated analytics tables - Active Agreements: {len(df_active_agreements)}, "
        f"Halfhourly periods: {len(df_halfhourly)}, "
        f"Product-days: {len(df_product_daily)}"
    )
    
    return {
        'active_agreements': df_active_agreements,
        'halfhourly_consumption': df_halfhourly,
        'daily_product_consumption': df_product_daily
    }

def load_data(data: Dict[str, Any], reference_date: str) -> None:
    """Load transformed data into PostgreSQL database."""
    logger.info("Starting data loading to PostgreSQL...")
    
    writer = PostgresWriter()
    
    # Create schema if it doesn't exist
    writer.create_analytics_schema()
    
    # Write transformed data
    writer.write_active_agreements(data['active_agreements'], reference_date)
    writer.write_halfhourly_consumption(data['halfhourly_consumption'])
    writer.write_daily_product_consumption(data['daily_product_consumption'])
    
    # Validate loaded data
    table_info = writer.get_table_info()
    for table, info in table_info.items():
        logger.info(
            f"Table {table}: {info['row_count']} rows, "
            f"last update: {info['last_update']}"
        )

def run_etl(reference_date: str = '2021-01-01') -> None:
    """
    Run the complete ETL pipeline.
    
    Args:
        reference_date: Reference date for active agreements analysis.
                       Defaults to '2021-01-01'.
    """
    start_time = time.time()
    logger.info(f"Starting ETL pipeline with reference date: {reference_date}")
    
    try:
        # Extract
        extracted_data = extract_data()
        logger.info("Data extraction completed")
        
        # Transform
        transformed_data = transform_data(extracted_data, reference_date)
        logger.info("Data transformation completed")
        
        # Load
        load_data(transformed_data, reference_date)
        logger.info("Data loading completed")
        
        duration = time.time() - start_time
        logger.info(f"ETL pipeline completed successfully in {duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    run_etl()

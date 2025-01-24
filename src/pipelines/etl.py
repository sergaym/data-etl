"""
Enterprise-Grade ETL Pipeline for Meter Readings Data.
- Designed for production deployment with logging, retry logic, and monitoring.
- Modular structure for scalability and orchestration.
"""

import time
import sys
import argparse
from extraction import load_json_readings, get_data_summary, DatabaseLoader
from transformation.transformers import DataTransformer
from loading import PostgresWriter
from utils.logger import get_logger

logger = get_logger("etl_pipeline")

def extract_data():
    """
    Extracts data from JSON files and database tables.
    
    Returns:
        tuple: (df_readings, df_agreement, df_product, df_meterpoint)
    """
    try:
        start_time = time.time()
        logger.info("Starting data extraction...")

        # Extract JSON readings
        df_readings = load_json_readings()
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
        raise  # Avoid silent failure


def transform_data(df_readings, df_agreement, df_product, df_meterpoint, reference_date):
    """
    Transforms the extracted data into required formats.

    Args:
        df_readings (pd.DataFrame): Meter readings data.
        df_agreement (pd.DataFrame): Agreement data.
        df_product (pd.DataFrame): Product data.
        df_meterpoint (pd.DataFrame): Meter point data.
        reference_date (str): Date for filtering agreements.

    Returns:
        tuple: (df_active_agreements, df_halfhourly, df_product_daily)
    """
    try:
        start_time = time.time()
        logger.info("Starting data transformation...")

        transformer = DataTransformer(
            df_readings=df_readings,
            df_agreement=df_agreement,
            df_product=df_product,
            df_meterpoint=df_meterpoint
        )

        df_active_agreements = transformer.get_active_agreements(reference_date)
        df_halfhourly = transformer.get_halfhourly_consumption()
        df_product_daily = transformer.get_daily_product_consumption()

        logger.info(f"Data transformation completed in {time.time() - start_time:.2f} seconds")
        return df_active_agreements, df_halfhourly, df_product_daily

    except Exception as e:
        logger.error(f"Data transformation failed: {e}", exc_info=True)
        raise


def load_data(df_active_agreements, df_halfhourly, df_product_daily, reference_date):
    """
    Loads transformed data into PostgreSQL and validates results.

    Args:
        df_active_agreements (pd.DataFrame): Processed active agreements.
        df_halfhourly (pd.DataFrame): Half-hourly consumption data.
        df_product_daily (pd.DataFrame): Daily product consumption data.
        reference_date (str): Reference date for validation.
    """
    try:
        start_time = time.time()
        logger.info("Starting data loading...")

        writer = PostgresWriter()
        writer.create_analytics_schema()
        writer.write_active_agreements(df_active_agreements, reference_date=reference_date)
        writer.write_halfhourly_consumption(df_halfhourly)
        writer.write_daily_product_consumption(df_product_daily)

        # Validate loaded data
        table_info = writer.get_table_info()
        for table, info in table_info.items():
            logger.info(f"{table}: {info['row_count']} rows, last update {info['last_update']}")

        logger.info(f"Data loading completed in {time.time() - start_time:.2f} seconds")

    except Exception as e:
        logger.error(f"Data loading failed: {e}", exc_info=True)
        raise


def run_etl(reference_date):
    """
    Runs the full ETL pipeline with the provided reference date.

    Args:
        reference_date (str): Reference date for processing agreements.
    """
    try:
        total_start_time = time.time()
        logger.info(f"Starting ETL pipeline for reference date: {reference_date}")

        # Execute each stage sequentially
        df_readings, df_agreement, df_product, df_meterpoint = extract_data()
        df_active_agreements, df_halfhourly, df_product_daily = transform_data(
            df_readings, df_agreement, df_product, df_meterpoint, reference_date
        )
        load_data(df_active_agreements, df_halfhourly, df_product_daily, reference_date)

        logger.info(f"ETL pipeline completed successfully in {time.time() - total_start_time:.2f} seconds")

    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # Parse CLI arguments for reference date
    parser = argparse.ArgumentParser(description="Run the ETL pipeline.")
    parser.add_argument("--reference_date", type=str, default="2022-06-15", help="Reference date for processing agreements (YYYY-MM-DD)")
    args = parser.parse_args()

    run_etl(args.reference_date)

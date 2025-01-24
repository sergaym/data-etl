"""
Playground for analysts to query transformed data from PostgreSQL database.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def get_db_connection():
    """Create database connection using environment variables."""
    load_dotenv()
    
    connection_string = (
        f"postgresql://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}"
        f"@{os.getenv('PGHOST')}/{os.getenv('PGDATABASE')}?sslmode=require"
    )
    return create_engine(connection_string)

def run_query(query: str) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a DataFrame.
    
    Args:
        query: SQL query string
    
    Returns:
        DataFrame containing query results
    """
    engine = get_db_connection()
    return pd.read_sql_query(query, engine)

def main():
    """Run example queries on the transformed data."""
    # Set the schema name from environment variable
    schema = os.getenv('ANALYTICS_SCHEMA', 'analytics')
    
    # Example 1: Get active agreements summary by product
    print("\n=== Active Agreements by Product ===")
    query = f"""
    SELECT 
        display_name,
        is_variable,
        COUNT(*) as agreement_count
    FROM {schema}.active_agreements
    GROUP BY display_name, is_variable
    ORDER BY agreement_count DESC
    """
    df_agreements = run_query(query)
    print(df_agreements)
    
    # Example 2: Get total consumption by day
    print("\n=== Daily Consumption Trends ===")
    query = f"""
    SELECT 
        DATE_TRUNC('day', datetime) as date,
        COUNT(DISTINCT meterpoint_count) as total_meters,
        SUM(total_consumption_kwh) as total_consumption,
        AVG(total_consumption_kwh) as avg_consumption
    FROM {schema}.halfhourly_consumption
    GROUP BY DATE_TRUNC('day', datetime)
    ORDER BY date
    """
    df_consumption = run_query(query)
    print(df_consumption)
    
    # Example 3: Get product performance metrics
    print("\n=== Product Performance Metrics ===")
    query = f"""
    SELECT 
        product_display_name,
        COUNT(DISTINCT date) as days_with_readings,
        AVG(meterpoint_count) as avg_daily_meters,
        SUM(total_consumption_kwh) as total_consumption,
        AVG(total_consumption_kwh) as avg_daily_consumption
    FROM {schema}.daily_product_consumption
    GROUP BY product_display_name
    ORDER BY total_consumption DESC
    """
    df_products = run_query(query)
    print(df_products)
    
    # Example 4: Get hourly consumption patterns
    print("\n=== Hourly Consumption Patterns ===")
    query = f"""
    SELECT 
        EXTRACT(HOUR FROM datetime) as hour_of_day,
        AVG(total_consumption_kwh) as avg_consumption,
        AVG(meterpoint_count) as avg_active_meters
    FROM {schema}.halfhourly_consumption
    GROUP BY EXTRACT(HOUR FROM datetime)
    ORDER BY hour_of_day
    """
    df_hourly = run_query(query)
    print(df_hourly)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")

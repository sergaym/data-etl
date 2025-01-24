"""
Script to explore and display the structure of the case study database.
"""

import os
import sys
from pathlib import Path

# Add the project root directory to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from src.data_ingestion import DatabaseLoader
import pandas as pd

# Configure pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', 20)

def explore_database():
    """Explore and display database structure and sample data."""
    
    print("\n=== Database Structure Explorer ===")
    db = DatabaseLoader()
    
    # 1. Get all table names
    tables = db.get_table_names()
    print(f"\nFound {len(tables)} tables in database:")
    for table in tables:
        print(f"  - {table}")
    
    # 2. Display schema for each table
    print("\n=== Table Schemas ===")
    for table in tables:
        print(f"\nTable: {table}")
        print("-" * (len(table) + 7))
        
        schema = db.get_table_schema(table)
        for column in schema:
            nullable_str = 'NULL' if column.get('nullable', True) else 'NOT NULL'
            print(f"  - {column['name']:<20} {str(column['type']):<15} {nullable_str}")
    
    # 3. Show sample data from each table
    print("\n=== Sample Data ===")
    for table in tables:
        print(f"\nTable: {table}")
        print("-" * (len(table) + 7))
        
        # Load first 5 rows from each table
        df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 5", db.engine)
        print(df)
        
        # Display row count
        count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table}", db.engine)
        print(f"\nTotal rows: {count['count'].iloc[0]:,}")
    
    # 4. Show relationships (foreign keys)
    print("\n=== Table Relationships ===")
    relationships = {
        'agreement': {
            'meterpoint_id': 'meterpoint.id',
            'product_id': 'product.id'
        }
    }
    
    for table, relations in relationships.items():
        print(f"\nTable: {table}")
        print("-" * (len(table) + 7))
        for column, reference in relations.items():
            print(f"  - {column} -> {reference}")

def main():
    try:
        explore_database()
    except Exception as e:
        print(f"Error exploring database: {e}")
        raise  # Re-raise the exception to see the full traceback

if __name__ == "__main__":
    main() 
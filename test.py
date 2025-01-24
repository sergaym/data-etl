"""
Main script for processing meter readings data.
"""

from src.data_ingestion import load_json_readings, get_data_summary, DatabaseLoader

def main():
    try:
        # Part 1: Process JSON files
        print("\n=== Processing JSON Files ===")
        df_readings = load_json_readings()
        readings_summary = get_data_summary(df_readings)
        
        print("\nReadings Data Summary:")
        print(f"Total number of readings: {readings_summary['total_readings']:,}")
        print(f"Number of unique meterpoints: {readings_summary['unique_meterpoints']:,}")
        print(f"Date range: {readings_summary['date_range']['start']} to {readings_summary['date_range']['end']}")
        print(f"Total consumption: {readings_summary['total_consumption']:,.2f}")
        print(f"Average consumption: {readings_summary['average_consumption']:.4f}")
        
        # Part 2: Process Database
        print("\n=== Processing Database ===")
        db = DatabaseLoader()
        
        # First, let's check the table structure
        print("\nTable Schemas:")
        for table in db.get_table_names():
            print(f"\n{table} columns:")
            for column in db.get_table_schema(table):
                print(f"  - {column['name']}: {column['type']}")
        
        # Get database summary
        db_summary = db.get_database_summary()
        print("\nDatabase Summary:")
        for table, count in db_summary.items():
            if table == 'agreement_date_range':
                print(f"Agreement date range: {count['start']} to {count['end']}")
            else:
                print(f"{table}: {count:,}")
        
        # Example: Get active agreements for a specific date
        sample_date = readings_summary['date_range']['start'].split()[0]  # Get date from readings
        active_agreements = db.get_active_agreements(sample_date)
        print(f"\nActive agreements on {sample_date}: {len(active_agreements):,}")
        
        # Show sample of joined data
        print("\nSample of agreements with product and meterpoint details:")
        print(active_agreements[['meterpoint_id', 'product_name', 'meterpoint_region']].head())
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()

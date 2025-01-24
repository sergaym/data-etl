"""
Main script for processing meter readings data.
"""

from src.data_ingestion import load_json_readings, get_data_summary, DatabaseLoader

def main():
    try:
        # Part 1: Process JSON files
        print("\n=== Processing JSON Files ===")
        df_readings = load_json_readings() ### first table!
        readings_summary = get_data_summary(df_readings)
        
        print("\nReadings Data Summary:")
        print(f"Total number of readings: {readings_summary['total_readings']:,}")
        print(f"Number of unique meterpoints: {readings_summary['unique_meterpoints']:,}")
        print(f"Date range: {readings_summary['date_range']['start']} to {readings_summary['date_range']['end']}")
        print(f"Total consumption: {readings_summary['total_consumption']:,.2f}")
        print(f"Average consumption: {readings_summary['average_consumption']:.4f}")
        print(df_readings)
        # Part 2: Process Database
        print("\n=== Processing Database ===")
        db = DatabaseLoader()
        # First, let's check the table structure
        print("\nTable Schemas:")

        for table in db.get_table_names():
            print(f"\n{table} columns:")
            for column in db.get_table_schema(table):
                print(f"  - {column['name']}: {column['type']}")
        
        """
        # Get database summary
        db_summary = db.get_database_summary()
        print("\nDatabase Summary:")
        for table, count in db_summary.items():
            if table == 'agreement_date_range':
                print(f"Agreement date range: {count['start']} to {count['end']}")
            else:
                print(f"{table}: {count:,}")
        """
        # Load all tables into separate DataFrames
        print("\n=== Loading All Database Tables ===")
        df_agreement = db.load_table('agreement')
        df_product = db.load_table('product')
        df_meterpoint = db.load_table('meterpoint')
        
        print("\nAgreement Table:")
        print(df_agreement.head())
        print(f"Shape: {df_agreement.shape}")
        
        print("\nProduct Table:")
        print(df_product.head())
        print(f"Shape: {df_product.shape}")
        
        print("\nMeterpoint Table:")
        print(df_meterpoint.head())
        print(f"Shape: {df_meterpoint.shape}")
        
    except Exception as e:
        print(f"Error: {e}")
if __name__ == "__main__":
    main()

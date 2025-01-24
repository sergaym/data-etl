"""
Main script for processing meter readings data.
"""

from src.data_ingestion import load_json_readings, get_data_summary, DatabaseLoader
from src.data_loading import PostgresWriter

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
        
        # Part 3: Transform Data
        print("\n=== Transforming Data ===")
        from src.data_transformation.transformers import DataTransformer
        
        transformer = DataTransformer(
            df_readings=df_readings,
            df_agreement=df_agreement,
            df_product=df_product,
            df_meterpoint=df_meterpoint
        )
        
        # Define reference date for analysis
        reference_date = '2022-06-15'
        
        # Generate analytics tables
        df_active_agreements = transformer.get_active_agreements(reference_date)
        df_halfhourly = transformer.get_halfhourly_consumption()
        df_product_daily = transformer.get_daily_product_consumption()
        
        # Print summaries of the analytics tables
        print("\nActive Agreements (2021-01-01):")
        print(df_active_agreements.head())
        print(f"Total active agreements: {len(df_active_agreements)}")
        
        print("\nHalf-hourly Consumption:")
        print(df_halfhourly.head())
        print(f"Total time periods: {len(df_halfhourly)}")
        
        print("\nDaily Product Consumption:")
        print(df_product_daily.head())
        print(f"Total product-days: {len(df_product_daily)}")
        ## pending to check units, if the total consumption is summed properly, if we are using properly the concept of 2021-01-01

        # Initialize writer
        writer = PostgresWriter()

        # Create analytics schema
        writer.create_analytics_schema()

        # Write transformed data
        writer.write_active_agreements(df_active_agreements, reference_date='2021-01-01')
        writer.write_halfhourly_consumption(df_halfhourly)
        writer.write_daily_product_consumption(df_product_daily)

        # Get information about loaded tables
        table_info = writer.get_table_info()
        for table, info in table_info.items():
            print(f"\n{table}:")
            print(f"Rows: {info['row_count']}")
            print(f"Last update: {info['last_update']}")
            print(f"Columns: {', '.join(info['columns'])}")
    except Exception as e:
        print(f"Error: {e}")
if __name__ == "__main__":
    main()

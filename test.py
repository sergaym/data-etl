"""
Main script for processing meter readings data.
"""

from src.data_ingestion import load_json_readings
from src.data_ingestion.json_loader import get_data_summary

def main():
    try:
        # Load all JSON files into a DataFrame
        df = load_json_readings()
        
        # Get and print data summary
        summary = get_data_summary(df)
        
        print("\nData Summary:")
        print(f"Total number of readings: {summary['total_readings']:,}")
        print(f"Number of unique meterpoints: {summary['unique_meterpoints']:,}")
        print(f"Date range: {summary['date_range']['start']} to {summary['date_range']['end']}")
        print(f"Total consumption: {summary['total_consumption']:,.2f}")
        print(f"Average consumption: {summary['average_consumption']:.4f}")
        
        print("\nFirst few readings:")
        print(df.head())
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()

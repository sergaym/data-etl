import json
import pandas as pd
from pathlib import Path

def read_json_files(folder_path='readings'):
    """
    Read all JSON files from the specified folder and combine them into a single DataFrame
    
    Args:
        folder_path (str): Path to the folder containing JSON files
        
    Returns:
        pd.DataFrame: Combined DataFrame with all readings
    """
    all_dataframes = []
    folder = Path(folder_path)
    
    for file_path in folder.glob('*.json'):
        try:
            with open(file_path, 'r') as file:
                json_content = json.load(file)
                # Create DataFrame directly from the data array using the columns specified in the JSON
                df = pd.DataFrame(json_content['data'], columns=json_content['columns'])
                all_dataframes.append(df)
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    if not all_dataframes:
        return pd.DataFrame()
    
    # Concatenate all DataFrames at once
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    # Convert timestamp to datetime
    combined_df['interval_start'] = pd.to_datetime(combined_df['interval_start'])
    
    # Sort by timestamp and meterpoint_id
    combined_df.sort_values(['meterpoint_id', 'interval_start'], inplace=True)
    
    return combined_df

if __name__ == "__main__":
    # Read all JSON files and create DataFrame
    df = read_json_files()
    
    # Print summary information
    print("\nDataFrame Summary:")
    print(f"Total number of readings: {len(df)}")
    print(f"Number of unique meterpoints: {df['meterpoint_id'].nunique()}")
    print(f"Date range: {df['interval_start'].min()} to {df['interval_start'].max()}")
    
    # Display first few rows
    print("\nFirst few readings:")
    print(df.head())

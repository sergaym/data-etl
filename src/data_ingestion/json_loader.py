"""
Module for loading and processing JSON meter reading data.
"""

import json
import logging
from pathlib import Path
from typing import Optional, List, Dict, Union
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def validate_json_structure(data: Dict) -> bool:
    """
    Validate the structure of loaded JSON data.
    
    Args:
        data: Dictionary containing the JSON data
        
    Returns:
        bool: True if valid, False otherwise
    """
    required_keys = {'columns', 'data'}
    if not all(key in data for key in required_keys):
        return False
    
    if not isinstance(data['columns'], list) or not isinstance(data['data'], list):
        return False
    
    expected_columns = {'interval_start', 'consumption_delta', 'meterpoint_id'}
    if not set(data['columns']) == expected_columns:
        return False
    
    return True

def load_json_readings(
    folder_path: Union[str, Path] = 'readings',
    sort_data: bool = True
) -> pd.DataFrame:
    """
    Load and process JSON files containing meter readings.
    
    Args:
        folder_path: Path to the folder containing JSON files
        sort_data: Whether to sort the final DataFrame by meterpoint_id and interval_start
        
    Returns:
        pd.DataFrame: Combined DataFrame with all readings
        
    Raises:
        ValueError: If no valid JSON files are found
        FileNotFoundError: If the folder_path doesn't exist
    """
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Directory not found: {folder_path}")
    
    all_dataframes: List[pd.DataFrame] = []
    processed_files = 0
    error_files = 0
    
    logger.info(f"Starting to process JSON files in {folder_path}")
    
    for file_path in folder.glob('*.json'):
        try:
            with open(file_path, 'r') as file:
                json_content = json.load(file)
                
                if not validate_json_structure(json_content):
                    logger.warning(f"Invalid JSON structure in file: {file_path}")
                    error_files += 1
                    continue
                
                df = pd.DataFrame(json_content['data'], columns=json_content['columns'])
                all_dataframes.append(df)
                processed_files += 1
                logger.debug(f"Successfully processed: {file_path}")
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error in {file_path}: {e}")
            error_files += 1
        except Exception as e:
            logger.error(f"Unexpected error processing {file_path}: {e}")
            error_files += 1
    
    if not all_dataframes:
        raise ValueError(f"No valid JSON files found in {folder_path}")
    
    logger.info(f"Processed {processed_files} files successfully, {error_files} files with errors")
    
    # Combine all DataFrames
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    # Convert timestamp to datetime
    combined_df['interval_start'] = pd.to_datetime(combined_df['interval_start'])
    
    # Sort if requested
    if sort_data:
        combined_df.sort_values(['meterpoint_id', 'interval_start'], inplace=True)
    
    logger.info(f"Final DataFrame shape: {combined_df.shape}")
    return combined_df

def get_data_summary(df: pd.DataFrame) -> Dict:
    """
    Generate a summary of the loaded data.
    
    Args:
        df: DataFrame containing the meter readings
        
    Returns:
        Dict containing summary statistics
    """
    return {
        'total_readings': len(df),
        'unique_meterpoints': df['meterpoint_id'].nunique(),
        'date_range': {
            'start': df['interval_start'].min(),
            'end': df['interval_start'].max()
        },
        'total_consumption': df['consumption_delta'].sum(),
        'average_consumption': df['consumption_delta'].mean()
    } 
"""
Module for loading and processing JSON meter readings.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any

import pandas as pd

# Configure logging
logger = logging.getLogger(__name__)

# Default paths
DEFAULT_READINGS_PATH = 'data/readings'

def load_json_readings(folder_path: str = DEFAULT_READINGS_PATH) -> pd.DataFrame:
    """
    Load and combine all JSON meter reading files.
    
    Args:
        folder_path: Path to the folder containing JSON files.
                    Defaults to 'data/readings'.
    
    Returns:
        DataFrame containing all meter readings
    
    Raises:
        FileNotFoundError: If readings directory doesn't exist
        ValueError: If no valid JSON files are found
    """
    path = Path(folder_path).resolve()
    logger.info(f"Starting to process JSON files in {path}")
    
    if not path.exists():
        raise FileNotFoundError(f"Readings directory not found: {path}")
    
    all_readings = []
    processed_files = 0
    error_files = 0
    
    for file_path in path.glob('*.json'):
        try:
            with open(file_path, 'r') as file:
                json_content = json.load(file)
                
                # Validate JSON structure
                if not all(key in json_content for key in ['columns', 'data']):
                    logger.warning(f"Invalid JSON structure in {file_path}")
                    error_files += 1
                    continue
                
                # Create DataFrame from JSON data
                df = pd.DataFrame(json_content['data'], columns=json_content['columns'])
                all_readings.append(df)
                processed_files += 1
                logger.debug(f"Successfully processed: {file_path}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON in {file_path}: {e}")
            error_files += 1
        except Exception as e:
            logger.error(f"Unexpected error processing {file_path}: {e}")
            error_files += 1
    
    if not all_readings:
        raise ValueError("No valid JSON files found")
    
    # Combine all readings
    combined_df = pd.concat(all_readings, ignore_index=True)
    
    # Convert timestamp to datetime
    combined_df['interval_start'] = pd.to_datetime(combined_df['interval_start'])
    
    logger.info(f"Processed {processed_files} files successfully, {error_files} files with errors")
    logger.info(f"Final DataFrame shape: {combined_df.shape}")
    
    return combined_df

def get_data_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate summary statistics for meter readings data.
    
    Args:
        df: DataFrame containing meter readings
    
    Returns:
        Dictionary containing summary statistics
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
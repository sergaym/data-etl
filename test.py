import json
import os
from pathlib import Path

def read_json_files(folder_path='readings'):
    """
    Read all JSON files from the specified folder
    
    Args:
        folder_path (str): Path to the folder containing JSON files
        
    Returns:
        list: List of dictionaries containing the JSON data
    """
    json_data = []
    
    # Create a Path object for the folder
    folder = Path(folder_path)
    
    # Iterate through all files in the folder
    for file_path in folder.glob('*.json'):
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
                json_data.append(data)
        except json.JSONDecodeError as e:
            print(f"Error reading {file_path}: {e}")
        except Exception as e:
            print(f"Unexpected error reading {file_path}: {e}")
            
    return json_data

# Example usage
if __name__ == "__main__":
    data = read_json_files()
    print(f"Found {len(data)} JSON files")
    # Print the content of each JSON file
    for i, item in enumerate(data, 1):
        print(f"\nFile {i}:")
        print(item)

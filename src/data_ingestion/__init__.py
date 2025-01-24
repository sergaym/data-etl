"""
Data ingestion module for processing meter reading data.
"""

from .json_loader import load_json_readings, get_data_summary
from .db_loader import DatabaseLoader

__all__ = [
    'load_json_readings',
    'get_data_summary',
    'DatabaseLoader'
] 
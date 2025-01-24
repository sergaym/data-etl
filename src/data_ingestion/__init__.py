"""
Data ingestion module for loading meter readings and database data.

This module provides functionality for loading and processing meter readings from JSON files
and accessing data from the SQLite database.
"""

from .db_loader import DatabaseLoader
from .json_loader import load_json_readings, get_data_summary

# Default paths
DEFAULT_DB_PATH = 'data/case_study.db'
DEFAULT_READINGS_PATH = 'data/readings'

__all__ = [
    'DatabaseLoader',
    'load_json_readings',
    'get_data_summary',
    'DEFAULT_DB_PATH',
    'DEFAULT_READINGS_PATH'
]
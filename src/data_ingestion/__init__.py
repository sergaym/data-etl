"""
Data ingestion module for processing meter reading data.
"""

from .json_loader import load_json_readings

__all__ = ['load_json_readings'] 
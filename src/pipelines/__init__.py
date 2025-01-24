"""
Pipeline package for data processing and ETL operations.

This package contains the core data processing pipelines used throughout the project,
including ETL (Extract, Transform, Load) operations for meter readings and related data.

Available functions:
    run_etl: Execute the complete ETL pipeline for processing meter readings
            and loading them into PostgreSQL.
"""

from typing import List
from .etl import run_etl

__all__: List[str] = [
    "run_etl",
]

"""
Data loading module for storing transformed data in PostgreSQL database.

This module provides functionality for writing analytics tables to a PostgreSQL database,
including active agreements, consumption aggregations, and product-based analytics.
"""

from .db_writer import PostgresWriter

__all__ = ['PostgresWriter']

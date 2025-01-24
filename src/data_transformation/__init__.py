"""
Data transformation module for processing meter readings and agreement data.

This module provides functionality for transforming raw data into analytics-ready tables,
including active agreements, consumption aggregations, and product-based analytics.
"""

from .transformers import DataTransformer

__all__ = ['DataTransformer']

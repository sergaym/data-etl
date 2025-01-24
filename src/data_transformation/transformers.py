"""
Module for transforming data into analytics tables.
"""

import pandas as pd
from datetime import datetime

class DataTransformer:
    """Class to transform raw data into analytics tables."""
    
    def __init__(self, df_readings: pd.DataFrame, df_agreement: pd.DataFrame, 
                 df_product: pd.DataFrame, df_meterpoint: pd.DataFrame):
        """
        Initialize transformer with required dataframes.
        
        Args:
            df_readings: DataFrame containing meter readings
            df_agreement: DataFrame containing agreement information
            df_product: DataFrame containing product information
            df_meterpoint: DataFrame containing meterpoint information
        """
        self.df_readings = df_readings
        self.df_agreement = df_agreement
        self.df_product = df_product
        self.df_meterpoint = df_meterpoint
        
        # Ensure datetime type for readings
        self.df_readings['interval_start'] = pd.to_datetime(self.df_readings['interval_start'])
        
    def get_active_agreements_2021(self) -> pd.DataFrame:
        """
        Get all agreements active on January 1st, 2021.
        
        Returns:
            DataFrame with one row per active agreement including agreement details
        """
        reference_date = '2021-01-01'
        
        # Filter for active agreements
        active_agreements = self.df_agreement[
            (self.df_agreement['agreement_valid_from'] <= reference_date) &
            (
                (self.df_agreement['agreement_valid_to'].isna()) |
                (self.df_agreement['agreement_valid_to'] >= reference_date)
            )
        ]
        
        # Merge with product information
        result = active_agreements.merge(
            self.df_product[['product_id', 'display_name', 'is_variable']],
            on='product_id',
            how='left'
        )
        
        # Select and rename columns
        final_columns = [
            'agreement_id',
            'meterpoint_id',
            'display_name',
            'is_variable'
        ]
        
        return result[final_columns]
    
    def get_halfhourly_consumption(self) -> pd.DataFrame:
        """
        Get aggregated consumption data for each half hour.
        
        Returns:
            DataFrame with one row per half hour including consumption metrics
        """
        # Group by interval_start
        result = self.df_readings.groupby('interval_start').agg({
            'meterpoint_id': 'nunique',
            'consumption_delta': 'sum'
        }).reset_index()
        
        # Rename columns
        result.columns = [
            'datetime',
            'meterpoint_count',
            'total_consumption_kwh'
        ]
        
        return result.sort_values('datetime')
    
    def get_daily_product_consumption(self) -> pd.DataFrame:
        """
        Get daily consumption aggregated by product.
        
        Returns:
            DataFrame with one row per product-day including consumption metrics
        """
        # Add date column to readings
        df_readings_daily = self.df_readings.copy()
        df_readings_daily['date'] = df_readings_daily['interval_start'].dt.date
        
        # Get active agreements for each reading date
        df_readings_with_agreement = df_readings_daily.merge(
            self.df_agreement,
            on='meterpoint_id',
            how='left'
        )
        
        # Filter for valid agreements
        df_readings_with_agreement = df_readings_with_agreement[
            (df_readings_with_agreement['agreement_valid_from'] <= df_readings_with_agreement['interval_start'].dt.date) &
            (
                (df_readings_with_agreement['agreement_valid_to'].isna()) |
                (df_readings_with_agreement['agreement_valid_to'] >= df_readings_with_agreement['interval_start'].dt.date)
            )
        ]
        
        # Add product information
        df_with_product = df_readings_with_agreement.merge(
            self.df_product[['product_id', 'display_name']],
            on='product_id',
            how='left'
        )
        
        # Group by product and date
        result = df_with_product.groupby(['display_name', 'date']).agg({
            'meterpoint_id': 'nunique',
            'consumption_delta': 'sum'
        }).reset_index()
        
        # Rename columns
        result.columns = [
            'product_display_name',
            'date',
            'meterpoint_count',
            'total_consumption_kwh'
        ]
        
        return result.sort_values(['date', 'product_display_name']) 
"""
Module for loading data from SQLite database.
"""

import logging
from pathlib import Path
from typing import Dict, Optional, List

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine

# Configure logging
logger = logging.getLogger(__name__)

class DatabaseLoader:
    """Class to handle database connections and queries."""
    
    def __init__(self, db_path: str = 'case_study.db'):
        """
        Initialize database connection.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = Path(db_path)
        self._engine: Optional[Engine] = None
        
    @property
    def engine(self) -> Engine:
        """Lazy loading of database engine."""
        if self._engine is None:
            if not self.db_path.exists():
                raise FileNotFoundError(f"Database file not found: {self.db_path}")
            
            connection_string = f"sqlite:///{self.db_path}"
            self._engine = create_engine(connection_string)
            logger.info(f"Connected to database: {self.db_path}")
        
        return self._engine
    
    def get_table_schema(self, table_name: str) -> List[Dict]:
        """
        Get the schema information for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of dictionaries containing column information
        """
        inspector = inspect(self.engine)
        return inspector.get_columns(table_name)
    
    def get_table_names(self) -> List[str]:
        """Get all table names from the database."""
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        return pd.read_sql_query(query, self.engine)['name'].tolist()
    
    def load_table(self, table_name: str) -> pd.DataFrame:
        """
        Load a complete table from the database.
        
        Args:
            table_name: Name of the table to load
            
        Returns:
            DataFrame containing the table data
        """
        logger.info(f"Loading table: {table_name}")
        return pd.read_sql_table(table_name, self.engine)
    
    def load_agreements_with_details(self) -> pd.DataFrame:
        """
        Load agreements with related product and meterpoint information.
        
        Returns:
            DataFrame with agreement details including product and meterpoint information
        """
        query = """
        SELECT 
            a.*,
            p.name as product_name,
            p.is_variable as product_is_variable,
            m.region as meterpoint_region
        FROM agreement a
        LEFT JOIN product p ON a.product_id = p.id
        LEFT JOIN meterpoint m ON a.meterpoint_id = m.id
        """
        
        logger.info("Loading agreements with product and meterpoint details")
        return pd.read_sql_query(query, self.engine)
    
    def get_active_agreements(self, date: str) -> pd.DataFrame:
        """
        Get all agreements that were active on a specific date.
        
        Args:
            date: Date string in YYYY-MM-DD format
            
        Returns:
            DataFrame with active agreements
        """
        # First, let's check the actual column names
        schema = self.get_table_schema('agreement')
        date_columns = [col['name'] for col in schema if 'date' in col['name'].lower()]
        
        if not date_columns:
            raise ValueError("Could not find date columns in agreement table")
        
        # Assuming we find the correct column names, update the query
        query = """
        SELECT 
            a.*,
            p.name as product_name,
            p.is_variable as product_is_variable,
            m.region as meterpoint_region
        FROM agreement a
        LEFT JOIN product p ON a.product_id = p.id
        LEFT JOIN meterpoint m ON a.meterpoint_id = m.id
        WHERE date(:date) BETWEEN a.start_date AND a.end_date
        """
        
        logger.info(f"Getting active agreements for date: {date}")
        return pd.read_sql_query(query, self.engine, params={'date': date})
    
    def get_meterpoint_history(self, meterpoint_id: str) -> pd.DataFrame:
        """
        Get complete history of agreements for a specific meterpoint.
        
        Args:
            meterpoint_id: ID of the meterpoint
            
        Returns:
            DataFrame with meterpoint's agreement history
        """
        query = """
        SELECT 
            a.*,
            p.name as product_name,
            p.is_variable as product_is_variable,
            m.region as meterpoint_region
        FROM agreement a
        LEFT JOIN product p ON a.product_id = p.id
        LEFT JOIN meterpoint m ON a.meterpoint_id = m.id
        WHERE a.meterpoint_id = :meterpoint_id
        ORDER BY a.start_date
        """
        
        logger.info(f"Getting history for meterpoint: {meterpoint_id}")
        return pd.read_sql_query(query, self.engine, params={'meterpoint_id': meterpoint_id})
    
    def get_database_summary(self) -> Dict:
        """
        Get summary statistics about the database.
        
        Returns:
            Dictionary containing summary statistics
        """
        summary = {}
        
        # Get table counts
        for table in self.get_table_names():
            count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table}", self.engine)
            summary[f"{table}_count"] = count['count'].iloc[0]
        
        # Get date ranges for agreements
        date_range = pd.read_sql_query(
            """
            SELECT 
                MIN(agreement_valid_from) as min_date,
                MAX(agreement_valid_to) as max_date 
            FROM agreement
            """,
            self.engine
        )
        summary['agreement_date_range'] = {
            'start': date_range['min_date'].iloc[0],
            'end': date_range['max_date'].iloc[0]
        }
        
        return summary 
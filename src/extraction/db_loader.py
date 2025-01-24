"""
Module for loading data from SQLite database.
"""

import logging
from pathlib import Path
from typing import Optional, List, Dict

import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine

# Configure logging
logger = logging.getLogger(__name__)

class DatabaseLoader:
    """Class to handle database connections and basic table loading."""
    
    # Update default paths to use data directory
    DEFAULT_DB_PATH = 'data/case_study.db'
    
    def __init__(self, 
                 db_path: str = DEFAULT_DB_PATH):
        """
        Initialize database connection and set up paths.
        
        Args:
            db_path: Path to the SQLite database file. Defaults to 'data/case_study.db'.
        
        Raises:
            ValueError: If db_path is empty or None
        """
        if not db_path:
            raise ValueError("Database path cannot be empty")
            
        self.db_path: Path = Path(db_path).resolve()
        self._engine: Optional[Engine] = None
        
        logger.debug(f"Initialized DatabaseLoader with db_path={self.db_path}")
        
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
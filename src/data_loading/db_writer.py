"""
Module for loading transformed data into PostgreSQL database.
"""

import os
import logging
from typing import Optional, Dict
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# Configure logging
logger = logging.getLogger(__name__)

class PostgresWriter:
    """Class to handle writing transformed data to PostgreSQL database."""
    
    def __init__(self, env_path: str = '.env'):
        """
        Initialize PostgreSQL connection using environment variables.
        
        Args:
            env_path: Path to .env file containing database credentials.
                     Defaults to '.env' in current directory.
        """
        # Load environment variables
        load_dotenv(env_path)
        
        # Get database connection parameters
        self.db_params = {
            'host': os.getenv('PGHOST'),
            'database': os.getenv('PGDATABASE'),
            'user': os.getenv('PGUSER'),
            'password': os.getenv('PGPASSWORD')
        }
        
        self._engine: Optional[Engine] = None
        
    @property
    def engine(self) -> Engine:
        """Lazy loading of database engine with SSL configuration."""
        if self._engine is None:
            connection_string = (
                f"postgresql://{self.db_params['user']}:{self.db_params['password']}"
                f"@{self.db_params['host']}/{self.db_params['database']}?sslmode=require"
            )
            self._engine = create_engine(connection_string)
            logger.info(f"Connected to PostgreSQL database: {self.db_params['database']}")
        
        return self._engine
    
    def create_analytics_schema(self) -> None:
        """Create analytics schema if it doesn't exist."""
        with self.engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics;"))
            conn.commit()
            logger.info("Analytics schema created or verified")
    
    def write_active_agreements(self, df: pd.DataFrame, reference_date: str) -> None:
        """
        Write active agreements data to PostgreSQL.
        
        Args:
            df: DataFrame containing active agreements data
            reference_date: The reference date for which agreements are active
        """
        table_name = 'active_agreements'
        schema = 'analytics'
        
        # Add metadata columns
        df['reference_date'] = pd.to_datetime(reference_date)
        df['loaded_at'] = datetime.now()
        
        df.to_sql(
            name=table_name,
            schema=schema,
            con=self.engine,
            if_exists='replace',
            index=False
        )
        logger.info(f"Wrote {len(df)} rows to {schema}.{table_name}")
    
    def write_halfhourly_consumption(self, df: pd.DataFrame) -> None:
        """
        Write half-hourly consumption data to PostgreSQL.
        
        Args:
            df: DataFrame containing half-hourly consumption data
        """
        table_name = 'halfhourly_consumption'
        schema = 'analytics'
        
        # Add metadata column
        df['loaded_at'] = datetime.now()
        
        df.to_sql(
            name=table_name,
            schema=schema,
            con=self.engine,
            if_exists='replace',
            index=False
        )
        logger.info(f"Wrote {len(df)} rows to {schema}.{table_name}")
    
    def write_daily_product_consumption(self, df: pd.DataFrame) -> None:
        """
        Write daily product consumption data to PostgreSQL.
        
        Args:
            df: DataFrame containing daily product consumption data
        """
        table_name = 'daily_product_consumption'
        schema = 'analytics'
        
        # Add metadata column
        df['loaded_at'] = datetime.now()
        
        df.to_sql(
            name=table_name,
            schema=schema,
            con=self.engine,
            if_exists='replace',
            index=False
        )
        logger.info(f"Wrote {len(df)} rows to {schema}.{table_name}")
    
    def get_table_info(self, schema: str = 'analytics') -> Dict:
        """
        Get information about tables in the analytics schema.
        
        Args:
            schema: Database schema to inspect. Defaults to 'analytics'.
        
        Returns:
            Dictionary containing table information including row counts
        """
        inspector = inspect(self.engine)
        info = {}
        
        for table_name in inspector.get_table_names(schema=schema):
            # Get row count
            query = text(f"SELECT COUNT(*) FROM {schema}.{table_name}")
            with self.engine.connect() as conn:
                row_count = conn.execute(query).scalar()
            
            # Get last update time
            query = text(f"SELECT MAX(loaded_at) FROM {schema}.{table_name}")
            with self.engine.connect() as conn:
                last_update = conn.execute(query).scalar()
            
            info[table_name] = {
                'row_count': row_count,
                'last_update': last_update,
                'columns': [col['name'] for col in inspector.get_columns(table_name, schema=schema)]
            }
        
        return info 
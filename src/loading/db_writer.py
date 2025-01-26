"""
Module for loading both raw and transformed data into PostgreSQL database.
"""

import os
import logging
from typing import Optional, Dict, List, Set
from datetime import datetime
from io import StringIO

import pandas as pd
from sqlalchemy import create_engine, inspect, text, MetaData, Table, Column, DateTime, String, Float, Integer
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv

# Configure logging
logger = logging.getLogger(__name__)

class PostgresWriter:
    """Class to handle writing both raw and transformed data to PostgreSQL database."""
    
    def __init__(self, env_path: str = '.env'):
        """
        Initialize PostgreSQL connection using environment variables.
        
        Args:
            env_path: Path to .env file containing database credentials.
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
        
        # Define schemas
        self.raw_schema = os.getenv('RAW_SCHEMA', 'raw_data')
        self.analytics_schema = os.getenv('ANALYTICS_SCHEMA', 'analytics')
        
        self._engine: Optional[Engine] = None
        self._metadata: Optional[MetaData] = None
        self._inspector: Optional[inspect] = None
        self._existing_schemas: Optional[Set[str]] = None
        
        logger.debug(f"Initialized PostgresWriter with schemas: raw={self.raw_schema}, analytics={self.analytics_schema}")
    
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
    
    @property
    def metadata(self) -> MetaData:
        """Lazy loading of SQLAlchemy metadata."""
        if self._metadata is None:
            self._metadata = MetaData()
        return self._metadata

    @property
    def inspector(self):
        """Lazy loading of SQLAlchemy inspector."""
        if self._inspector is None:
            self._inspector = inspect(self.engine)
        return self._inspector

    @property
    def existing_schemas(self) -> Set[str]:
        """Get existing schemas in the database."""
        if self._existing_schemas is None:
            query = text("SELECT schema_name FROM information_schema.schemata")
            with self.engine.connect() as conn:
                result = conn.execute(query)
                self._existing_schemas = {row[0] for row in result}
        return self._existing_schemas

    def ensure_schema_exists(self, schema_name: str) -> None:
        """Create schema if it doesn't exist."""
        if schema_name not in self.existing_schemas:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA {schema_name};"))
                conn.commit()
                self._existing_schemas.add(schema_name)
                logger.info(f"Created schema: {schema_name}")
        else:
            logger.debug(f"Schema already exists: {schema_name}")

    def create_schemas(self) -> None:
        """Create raw_data and analytics schemas if they don't exist."""
        with self.engine.connect() as conn:
            # Create raw_data schema
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.raw_schema};"))
            # Create analytics schema
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.analytics_schema};"))
            conn.commit()
            logger.info(f"Schemas '{self.raw_schema}' and '{self.analytics_schema}' created or verified")
    
    def ensure_raw_tables_exist(self) -> None:
        """Create raw tables if they don't exist."""
        # Check if tables already exist
        existing_tables = self.inspector.get_table_names(schema=self.raw_schema)
        
        # Define raw_meter_readings table if it doesn't exist
        if 'raw_meter_readings' not in existing_tables:
            Table('raw_meter_readings', self.metadata,
                Column('interval_start', DateTime, nullable=False),
                Column('consumption_delta', Float, nullable=False),
                Column('meterpoint_id', String, nullable=False),
                Column('loaded_at', DateTime, default=datetime.now),
                schema=self.raw_schema,
                extend_existing=True,
                # Add composite primary key
                primary_key=['meterpoint_id', 'interval_start']
            )
        
        # Define raw_agreements table if it doesn't exist
        if 'raw_agreements' not in existing_tables:
            Table('raw_agreements', self.metadata,
                Column('agreement_id', Integer, primary_key=True),
                Column('agreement_valid_from', DateTime, nullable=False),
                Column('agreement_valid_to', DateTime),
                Column('product_id', String, nullable=False),
                Column('meterpoint_id', String, nullable=False),
                Column('account_id', String, nullable=False),
                Column('loaded_at', DateTime, default=datetime.now),
                schema=self.raw_schema,
                extend_existing=True
            )
        
        # Define raw_products table if it doesn't exist
        if 'raw_products' not in existing_tables:
            Table('raw_products', self.metadata,
                Column('display_name', String, nullable=False),
                Column('is_variable', Integer, nullable=False),
                Column('id', Integer, primary_key=True),
                Column('product_id', String, nullable=False),
                Column('loaded_at', DateTime, default=datetime.now),
                schema=self.raw_schema,
                extend_existing=True
            )
        
        # Define raw_meterpoints table if it doesn't exist
        if 'raw_meterpoints' not in existing_tables:
            Table('raw_meterpoints', self.metadata,
                Column('region', String, nullable=False),
                Column('meterpoint_id', String, primary_key=True),
                Column('loaded_at', DateTime, default=datetime.now),
                schema=self.raw_schema,
                extend_existing=True
            )
        
        # Create all tables
        self.metadata.create_all(self.engine)
        logger.info("Raw data tables created or verified")

    def get_latest_reading_timestamp(self) -> Optional[datetime]:
        """Get the latest timestamp from raw_meter_readings."""
        # First check if table exists
        if 'raw_meter_readings' not in self.inspector.get_table_names(schema=self.raw_schema):
            logger.debug("raw_meter_readings table does not exist yet")
            return None
            
        # If table exists, get the latest timestamp
        query = text(f"""
            SELECT MAX(interval_start) 
            FROM {self.raw_schema}.raw_meter_readings
        """)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query).scalar()
            return result
        except Exception as e:
            logger.debug(f"Error getting latest timestamp: {e}")
            return None

    def load_raw_readings(self, df: pd.DataFrame) -> None:
        """Load raw meter readings into PostgreSQL."""
        try:
            # Ensure schema exists
            self.ensure_schema_exists(self.raw_schema)
            
            # Create table if not exists
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.raw_schema}.raw_meter_readings (
                interval_start TIMESTAMP NOT NULL,
                consumption_delta FLOAT NOT NULL,
                meterpoint_id TEXT NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (meterpoint_id, interval_start)
            );
            """
            
            # Execute table creation in a separate connection
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
                logger.info(f"Ensured table {self.raw_schema}.raw_meter_readings exists")
            
            # Add loaded_at timestamp
            df['loaded_at'] = datetime.now()
            
            # Use copy_from for efficient bulk loading
            output = StringIO()
            df[['interval_start', 'consumption_delta', 'meterpoint_id', 'loaded_at']].to_csv(
                output, 
                sep='\t', 
                header=False, 
                index=False,
                date_format='%Y-%m-%d %H:%M:%S'  # Ensure proper datetime format
            )
            output.seek(0)
            
            # Get raw connection and perform copy
            raw_conn = self.engine.raw_connection()
            try:
                with raw_conn.cursor() as cur:
                    # Ensure we're in the correct schema
                    cur.execute(f"SET search_path TO {self.raw_schema}")
                    
                    # Perform the copy operation
                    cur.copy_from(
                        output,
                        'raw_meter_readings',  # Remove schema prefix as we set search_path
                        columns=['interval_start', 'consumption_delta', 'meterpoint_id', 'loaded_at'],
                        sep='\t'
                    )
                    raw_conn.commit()
                    logger.info(f"Successfully loaded {len(df)} rows into raw_meter_readings")
            except Exception as e:
                raw_conn.rollback()
                logger.error(f"Failed during copy operation: {str(e)}")
                raise
            finally:
                raw_conn.close()
                
        except Exception as e:
            logger.error(f"Failed to load raw readings: {str(e)}")
            raise

    def load_raw_reference_data(self, table_name: str, df: pd.DataFrame) -> None:
        """Load reference data (agreements, products, meterpoints) into PostgreSQL."""
        try:
            # Ensure schema exists
            self.ensure_schema_exists(self.raw_schema)
            
            # Add loaded_at timestamp
            df['loaded_at'] = datetime.now()
            
            # Use pandas to_sql for smaller reference tables
            df.to_sql(
                name=table_name,
                schema=self.raw_schema,
                con=self.engine,
                if_exists='replace',
                index=False
            )
            
            logger.info(f"Loaded {len(df)} rows into {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
            raise

    def write_active_agreements(self, df: pd.DataFrame, reference_date: str) -> None:
        """
        Write active agreements data to PostgreSQL.
        
        Args:
            df: DataFrame containing active agreements data
            reference_date: The reference date for which agreements are active
        """
        table_name = 'active_agreements'
        
        # Add metadata columns
        df['reference_date'] = pd.to_datetime(reference_date)
        df['loaded_at'] = datetime.now()
        
        df.to_sql(
            name=table_name,
            schema=self.analytics_schema,
            con=self.engine,
            if_exists='replace',
            index=False
        )
        logger.info(f"Wrote {len(df)} rows to {self.analytics_schema}.{table_name}")
    
    def write_halfhourly_consumption(self, df: pd.DataFrame) -> None:
        """
        Write half-hourly consumption data to PostgreSQL.
        
        Args:
            df: DataFrame containing half-hourly consumption data
        """
        table_name = 'halfhourly_consumption'
        
        # Add metadata column
        df['loaded_at'] = datetime.now()
        
        df.to_sql(
            name=table_name,
            schema=self.analytics_schema,
            con=self.engine,
            if_exists='replace',
            index=False
        )
        logger.info(f"Wrote {len(df)} rows to {self.analytics_schema}.{table_name}")
    
    def write_daily_product_consumption(self, df: pd.DataFrame) -> None:
        """
        Write daily product consumption data to PostgreSQL.
        
        Args:
            df: DataFrame containing daily product consumption data
        """
        table_name = 'daily_product_consumption'
        
        # Add metadata column
        df['loaded_at'] = datetime.now()
        
        df.to_sql(
            name=table_name,
            schema=self.analytics_schema,
            con=self.engine,
            if_exists='replace',
            index=False
        )
        logger.info(f"Wrote {len(df)} rows to {self.analytics_schema}.{table_name}")
    
    def get_table_info(self, schema: Optional[str] = None) -> Dict:
        """
        Get information about tables in the specified schema.
        
        Args:
            schema: Database schema to inspect. Defaults to the configured schema.
        
        Returns:
            Dictionary containing table information including row counts
        """
        schema = schema or self.analytics_schema
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
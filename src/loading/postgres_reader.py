from typing import Dict
import pandas as pd
from sqlalchemy.engine import Connection
from src.utils.logger import setup_logger

logger = setup_logger("postgres_reader")

class PostgresReader:
    """Class to handle reading operations from the PostgreSQL data warehouse."""
    
    def __init__(self, engine, raw_schema: str = 'raw', analytics_schema: str = 'analytics'):
        self.engine = engine
        self.raw_schema = raw_schema
        self.analytics_schema = analytics_schema
    
    def read_raw_tables(self) -> Dict[str, pd.DataFrame]:
        """Read all raw tables from PostgreSQL data warehouse."""
        try:
            logger.info("Reading raw tables from data warehouse...")
            with self.engine.connect() as conn:
                return {
                    'readings': self._read_table(conn, 'raw_meter_readings', self.raw_schema),
                    'agreement': self._read_table(conn, 'raw_agreements', self.raw_schema),
                    'product': self._read_table(conn, 'raw_products', self.raw_schema),
                    'meterpoint': self._read_table(conn, 'raw_meterpoints', self.raw_schema)
                }
        except Exception as e:
            logger.error(f"Failed to read raw tables: {e}", exc_info=True)
            raise
    
    def read_analytics_tables(self) -> Dict[str, pd.DataFrame]:
        """Read transformed tables from analytics schema."""
        try:
            logger.info("Reading analytics tables from data warehouse...")
            with self.engine.connect() as conn:
                return {
                    'active_agreements': self._read_table(conn, 'active_agreements', self.analytics_schema),
                    'halfhourly_consumption': self._read_table(conn, 'halfhourly_consumption', self.analytics_schema),
                    'daily_product_consumption': self._read_table(conn, 'daily_product_consumption', self.analytics_schema)
                }
        except Exception as e:
            logger.error(f"Failed to read analytics tables: {e}", exc_info=True)
            raise
    
    def _read_table(self, conn: Connection, table_name: str, schema: str) -> pd.DataFrame:
        """Read a single table from PostgreSQL."""
        return pd.read_sql_table(table_name, conn, schema=schema) 
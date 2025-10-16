"""
Database utility functions for the data engineering capstone project.

This module provides database connection management, query execution,
and connection testing functionality.
"""

import os
import yaml
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import logging
from contextlib import contextmanager

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Manages database connections and operations for the capstone project.
    
    This class handles PostgreSQL connections, provides methods for
    executing queries, and manages connection lifecycle.
    """
    
    def __init__(self, environment='development'):
        """
        Initialize database manager with environment configuration.
        
        Args:
            environment (str): Environment name (development, test, production)
        """
        self.environment = environment
        self.config = self._load_config()
        self.connection_string = self._build_connection_string()
        
    def _load_config(self):
        """Load database configuration from YAML file."""
        config_path = os.path.join('config', 'database.yml')
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config[self.environment]
    
    def _build_connection_string(self):
        """Build SQLAlchemy connection string from configuration."""
        password = os.getenv('POSTGRES_PASSWORD')
        return f"postgresql://{self.config['username']}:{password}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
    
    def test_connection(self):
        """
        Test database connection and return status.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            engine = create_engine(self.connection_string)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT version();"))
                version = result.fetchone()[0]
                logger.info(f"✅ Database connection successful!")
                logger.info(f"📊 PostgreSQL version: {version}")
                return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def get_engine(self):
        """Get SQLAlchemy engine for database operations."""
        return create_engine(self.connection_string)
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        
        Yields:
            connection: SQLAlchemy connection object
        """
        engine = self.get_engine()
        conn = engine.connect()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
            raise
        finally:
            conn.close()
    
    def execute_query(self, query, params=None):
        """
        Execute SQL query and return results as pandas DataFrame.
        
        Args:
            query (str): SQL query to execute
            params (dict): Query parameters for parameterized queries
            
        Returns:
            pd.DataFrame: Query results
        """
        try:
            engine = self.get_engine()
            return pd.read_sql(query, engine, params=params)
        except Exception as e:
            logger.error(f"❌ Query execution failed: {e}")
            return None
    
    def bulk_insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = 'public',
        if_exists: str = 'append',
        chunksize: int = 10000
    ) -> int:
        """
        Efficiently insert a pandas DataFrame into a PostgreSQL table in chunks.
        
        Args:
            df: DataFrame to insert
            table_name: Target table name
            schema: Target schema
            if_exists: 'append' or 'replace'
            chunksize: Number of rows per insert chunk
            
        Returns:
            int: Total number of records inserted
        """
        total_inserted = 0
        engine = self.get_engine()

        # Create unbound MetaData and reflect table
        meta = MetaData()
        table = Table(table_name, meta, autoload_with=engine, schema=schema)

        # Convert DataFrame to list of dictionaries (records)
        records = df.to_dict(orient='records')

        try:
            with engine.begin() as conn:
                for i in range(0, len(records), chunksize):
                    chunk = records[i:i + chunksize]
                    conn.execute(table.insert(), chunk)
                    total_inserted += len(chunk)
                    logger.info(f"Inserted chunk {i // chunksize + 1} ({len(chunk)} records)")
            
            logger.info(f"✅ Successfully inserted {total_inserted:,} records into {schema}.{table_name}")
            return total_inserted

        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to insert records: {e}")
            raise
    
    def execute_sql_file(self, file_path):
        """
        Execute SQL commands from a file.
        
        Args:
            file_path (str): Path to SQL file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with open(file_path, 'r') as file:
                sql_commands = file.read()
            
            with self.get_connection() as conn:
                conn.execute(text(sql_commands))
            
            logger.info(f"✅ Successfully executed SQL file: {file_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to execute SQL file {file_path}: {e}")
            return False

if __name__ == "__main__":
    # Test database connection
    db = DatabaseManager()
    db.test_connection()
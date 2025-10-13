"""
Database utility functions for the data engineering capstone project.

This module provides database connection management, query execution,
and connection testing functionality.
"""

import os
import yaml
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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
                print(f"✅ Database connection successful!")
                print(f"📊 PostgreSQL version: {version}")
                return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def get_engine(self):
        """Get SQLAlchemy engine for database operations."""
        return create_engine(self.connection_string)
    
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
            print(f"❌ Query execution failed: {e}")
            return None

if __name__ == "__main__":
    # Test database connection
    db = DatabaseManager()
    db.test_connection()
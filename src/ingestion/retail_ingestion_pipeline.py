"""
Advanced data ingestion pipeline for UCI Online Retail dataset.

This module implements a production-ready ingestion system with comprehensive
error handling, data validation, logging, and support for the Medallion Architecture.
"""

import os
import hashlib
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from uuid import uuid4
import traceback

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
from psycopg2.extras import execute_values

from src.utils.database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('logs/ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataIngestionError(Exception):
    """Custom exception for data ingestion errors."""
    pass

class RetailDataIngestionPipeline:
    """
    Production-ready data ingestion pipeline for retail data.
    
    This pipeline implements the Bronze layer of the Medallion Architecture,
    providing robust data ingestion with comprehensive error handling,
    validation, and monitoring capabilities.
    """
    
    def __init__(self, environment='development'):
        """Initialize the ingestion pipeline."""
        self.environment = environment
        self.db_manager = DatabaseManager(environment)
        self.engine = self.db_manager.get_engine()
        
        # Pipeline configuration
        self.batch_size = 10000
        self.max_retries = 3
        self.retry_delay = 5
        
        # Data validation thresholds
        self.validation_config = {
            'max_null_percentage': 0.1,  # 10% max nulls allowed
            'min_records': 100,           # Minimum records to process
            'max_price': 10000,          # Maximum unit price
            'date_range': ('2009-01-01', '2012-12-31')  # Valid date range
        }
        
        logger.info(f"🔄 Initialized RetailDataIngestionPipeline for {environment}")
    
    def generate_record_hash(self, record: Dict) -> str:
        """
        Generate a hash for a data record to detect duplicates.
        
        Args:
            record (Dict): Data record dictionary
            
        Returns:
            str: SHA-256 hash of the record
        """
        # Create a consistent string representation
        record_str = '|'.join([
            str(record.get('invoice', '')),
            str(record.get('stock_code', '')),
            str(record.get('quantity', '')),
            str(record.get('invoice_date', '')),
            str(record.get('unit_price', '')),
            str(record.get('customer_id', ''))
        ])
        
        return hashlib.sha256(record_str.encode()).hexdigest()
    
    def validate_data_quality(self, df: pd.DataFrame) -> Dict[str, any]:
        """
        Perform comprehensive data quality validation.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            
        Returns:
            Dict: Validation results with metrics and issues
        """
        logger.info("🔍 Starting data quality validation...")
        
        validation_results = {
            'is_valid': True,
            'total_records': len(df),
            'issues': [],
            'metrics': {},
            'warnings': []
        }
        
        try:
            # Check minimum record count
            if len(df) < self.validation_config['min_records']:
                validation_results['issues'].append(
                    f"Insufficient records: {len(df)} < {self.validation_config['min_records']}"
                )
                validation_results['is_valid'] = False
            
            # Check for required columns
            required_columns = ['InvoiceNo', 'StockCode', 'Quantity', 'InvoiceDate', 'UnitPrice', 'Country']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                validation_results['issues'].append(f"Missing required columns: {missing_columns}")
                validation_results['is_valid'] = False
            
            # Calculate null percentages
            null_percentages = df.isnull().mean()
            validation_results['metrics']['null_percentages'] = null_percentages.to_dict()
            
            for col, null_pct in null_percentages.items():
                if null_pct > self.validation_config['max_null_percentage']:
                    validation_results['warnings'].append(
                        f"High null percentage in {col}: {null_pct:.2%}"
                    )
            
            # Validate data types and ranges
            if 'UnitPrice' in df.columns:
                invalid_prices = df['UnitPrice'] < 0
                negative_price_count = invalid_prices.sum()
                if negative_price_count > 0:
                    validation_results['warnings'].append(
                        f"Found {negative_price_count} records with negative prices"
                    )
                
                max_price = df['UnitPrice'].max()
                if max_price > self.validation_config['max_price']:
                    validation_results['warnings'].append(
                        f"Unusually high price detected: £{max_price}"
                    )
            
            # Validate quantities
            if 'Quantity' in df.columns:
                zero_quantities = (df['Quantity'] == 0).sum()
                if zero_quantities > 0:
                    validation_results['warnings'].append(
                        f"Found {zero_quantities} records with zero quantity"
                    )
            
            # Validate date range
            if 'InvoiceDate' in df.columns:
                try:
                    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
                    min_date = df['InvoiceDate'].min()
                    max_date = df['InvoiceDate'].max()
                    
                    validation_results['metrics']['date_range'] = {
                        'min_date': str(min_date),
                        'max_date': str(max_date)
                    }
                    
                    config_min = pd.to_datetime(self.validation_config['date_range'][0])
                    config_max = pd.to_datetime(self.validation_config['date_range'][1])
                    
                    if min_date < config_min or max_date > config_max:
                        validation_results['warnings'].append(
                            f"Dates outside expected range: {min_date} to {max_date}"
                        )
                        
                except Exception as e:
                    validation_results['issues'].append(f"Date validation error: {str(e)}")
            
            # Calculate additional metrics
            validation_results['metrics'].update({
                'unique_invoices': df['InvoiceNo'].nunique() if 'InvoiceNo' in df.columns else 0,
                'unique_products': df['StockCode'].nunique() if 'StockCode' in df.columns else 0,
                'unique_customers': df['CustomerID'].nunique() if 'CustomerID' in df.columns else 0,
                'unique_countries': df['Country'].nunique() if 'Country' in df.columns else 0,
                'total_revenue': (df['Quantity'] * df['UnitPrice']).sum() if all(col in df.columns for col in ['Quantity', 'UnitPrice']) else 0
            })
            
            logger.info(f"✅ Data validation completed. Valid: {validation_results['is_valid']}")
            logger.info(f"📊 Records: {validation_results['total_records']:,}")
            logger.info(f"⚠️  Issues: {len(validation_results['issues'])}")
            logger.info(f"💡 Warnings: {len(validation_results['warnings'])}")
            
        except Exception as e:
            logger.error(f"❌ Error during data validation: {str(e)}")
            validation_results['issues'].append(f"Validation error: {str(e)}")
            validation_results['is_valid'] = False
        
        return validation_results
    
    def create_load_metadata_record(self, source_name: str, file_path: str = None) -> str:
        """
        Create a load metadata record to track the ingestion process.
        
        Args:
            source_name (str): Name of the data source
            file_path (str): Path to the source file
            
        Returns:
            str: Load ID for tracking this ingestion
        """
        load_id = str(uuid4())
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO bronze.load_metadata 
                    (load_id, source_name, file_path, load_start_time, load_status)
                    VALUES (:load_id, :source_name, :file_path, :start_time, 'STARTED')
                """), {
                    'load_id': load_id,
                    'source_name': source_name,
                    'file_path': file_path,
                    'start_time': datetime.now()
                })
                conn.commit()
                logger.info(f"📝 Created load metadata record: {load_id}")
                
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to create load metadata: {str(e)}")
            raise DataIngestionError(f"Failed to create load metadata: {str(e)}")
        
        return load_id
    
    def update_load_metadata(self, load_id: str, status: str, records_loaded: int = 0, 
                           records_failed: int = 0, error_message: str = None):
        """
        Update load metadata with completion status and statistics.
        
        Args:
            load_id (str): Load ID to update
            status (str): Final status (COMPLETED, FAILED)
            records_loaded (int): Number of successfully loaded records
            records_failed (int): Number of failed records
            error_message (str): Error message if failed
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    UPDATE bronze.load_metadata 
                    SET load_end_time = :end_time,
                        load_status = :status,
                        records_loaded = :records_loaded,
                        records_failed = :records_failed,
                        error_message = :error_message
                    WHERE load_id = :load_id
                """), {
                    'load_id': load_id,
                    'end_time': datetime.now(),
                    'status': status,
                    'records_loaded': records_loaded,
                    'records_failed': records_failed,
                    'error_message': error_message
                })
                conn.commit()
                logger.info(f"📝 Updated load metadata: {load_id} - {status}")
                
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to update load metadata: {str(e)}")
    
    def load_data_to_bronze(self, df: pd.DataFrame, source_file: str, load_id: str) -> Tuple[int, int]:
        """
        Load data into Bronze layer with batch processing, duplicate prevention, and error handling.

        Args:
            df (pd.DataFrame): Data to load
            source_file (str): Source file name
            load_id (str): Load ID for tracking this ingestion run

        Returns:
            Tuple[int, int]: (records_loaded, records_failed)
        """
    
        logger.info(f"🔄 Loading {len(df):,} records to Bronze layer...")

        records_loaded = 0
        records_failed = 0

        try:
            df_copy = df.copy()

            # Standardize column names to match database schema
            column_mapping = {
                'InvoiceNo': 'invoice',
                'StockCode': 'stock_code',
                'Description': 'description',
                'Quantity': 'quantity',
                'InvoiceDate': 'invoice_date',
                'UnitPrice': 'unit_price',
                'CustomerID': 'customer_id',
                'Country': 'country'
            }
            df_copy = df_copy.rename(columns=column_mapping)

            # Add audit columns
            df_copy['load_timestamp'] = datetime.now()
            df_copy['source_file'] = source_file
            df_copy['load_id'] = load_id

            # Generate record hashes
            df_copy['record_hash'] = df_copy.apply(
                lambda row: self.generate_record_hash(row.to_dict()), axis=1
            )

            # Fetch existing record_hashes to prevent duplicates
            existing_hashes = set(pd.read_sql(
                "SELECT record_hash FROM bronze.retail_raw",
                self.engine
            )['record_hash'].tolist())

            # Filter out duplicates
            df_to_insert = df_copy[~df_copy['record_hash'].isin(existing_hashes)]
            skipped = len(df_copy) - len(df_to_insert)
            if skipped > 0:
                logger.warning(f"⚠️  Skipped {skipped} duplicate records based on record_hash")

            # Batch insert
            total_batches = (len(df_to_insert) + self.batch_size - 1) // self.batch_size

            for batch_num in range(total_batches):
                start_idx = batch_num * self.batch_size
                end_idx = min((batch_num + 1) * self.batch_size, len(df_to_insert))
                batch_df = df_to_insert.iloc[start_idx:end_idx]

                if batch_df.empty:
                    continue

                logger.info(f"📦 Processing batch {batch_num + 1}/{total_batches} ({len(batch_df)} records)")

                try:
                    # Use SQLAlchemy multi insert
                    batch_df.to_sql(
                        'retail_raw',
                        self.engine,
                        schema='bronze',
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    records_loaded += len(batch_df)
                    logger.info(f"✅ Batch {batch_num + 1} loaded successfully")

                except Exception as e:
                    logger.error(f"❌ Failed to load batch {batch_num + 1}: {str(e)}")
                    records_failed += len(batch_df)

                    # Insert row by row to identify failing records
                    for idx, row in batch_df.iterrows():
                        try:
                            row_df = pd.DataFrame([row])
                            row_df.to_sql(
                                'retail_raw',
                                self.engine,
                                schema='bronze',
                                if_exists='append',
                                index=False
                            )
                            records_loaded += 1
                            records_failed -= 1
                        except Exception as row_error:
                            logger.error(f"❌ Failed to load record {idx}: {str(row_error)}")

            success_rate = (records_loaded / len(df)) * 100 if len(df) > 0 else 0
            logger.info(f"✅ Bronze layer loading completed:")
            logger.info(f"   📊 Total records: {len(df):,}")
            logger.info(f"   ✅ Loaded: {records_loaded:,}")
            logger.info(f"   ❌ Failed: {records_failed:,}")
            logger.info(f"   📈 Success rate: {success_rate:.1f}%")

        except Exception as e:
            logger.error(f"💥 Critical error during Bronze loading: {str(e)}")
            logger.error(traceback.format_exc())
            records_failed = len(df) - records_loaded

        return records_loaded, records_failed
    
    def ingest_retail_data(self, file_path: str) -> Dict[str, any]:
        """
        Main ingestion method that orchestrates the entire process.
        
        Args:
            file_path (str): Path to the data file to ingest
            
        Returns:
            Dict: Ingestion results and statistics
        """
        logger.info(f"🚀 Starting retail data ingestion from: {file_path}")
        
        results = {
            'status': 'STARTED',
            'load_id': None,
            'file_path': file_path,
            'start_time': datetime.now(),
            'end_time': None,
            'records_processed': 0,
            'records_loaded': 0,
            'records_failed': 0,
            'validation_results': None,
            'error_message': None
        }
        
        load_id = None
        
        try:
            # Step 1: Create load metadata record
            source_name = f"UCI_Online_Retail_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            load_id = self.create_load_metadata_record(source_name, file_path)
            results['load_id'] = load_id
            
            # Step 2: Load and validate data
            logger.info("📖 Reading data file...")
            
            if file_path.endswith('.parquet'):
                df = pd.read_parquet(file_path)
            elif file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                raise DataIngestionError(f"Unsupported file format: {file_path}")
            
            logger.info(f"📊 Loaded {len(df):,} records from file")
            results['records_processed'] = len(df)
            
            # Step 3: Data quality validation
            validation_results = self.validate_data_quality(df)
            results['validation_results'] = validation_results
            
            if not validation_results['is_valid']:
                error_msg = f"Data validation failed: {', '.join(validation_results['issues'])}"
                raise DataIngestionError(error_msg)
            
            # Step 4: Load to Bronze layer
            records_loaded, records_failed = self.load_data_to_bronze(df, file_path, load_id)
            results['records_loaded'] = records_loaded
            results['records_failed'] = records_failed
            
            # Step 5: Update metadata and finalize
            if records_failed == 0:
                results['status'] = 'COMPLETED'
                self.update_load_metadata(load_id, 'COMPLETED', records_loaded, records_failed)
            else:
                results['status'] = 'COMPLETED_WITH_ERRORS'
                self.update_load_metadata(load_id, 'COMPLETED', records_loaded, records_failed,
                                        f"{records_failed} records failed to load")
            
            results['end_time'] = datetime.now()
            duration = (results['end_time'] - results['start_time']).total_seconds()
            
            logger.info("🎉 Retail data ingestion completed successfully!")
            logger.info(f"⏱️  Duration: {duration:.1f} seconds")
            logger.info(f"📊 Final Statistics:")
            logger.info(f"   Total processed: {results['records_processed']:,}")
            logger.info(f"   Successfully loaded: {results['records_loaded']:,}")
            logger.info(f"   Failed: {results['records_failed']:,}")
            logger.info(f"   Load ID: {load_id}")
            
        except Exception as e:
            error_message = str(e)
            logger.error(f"💥 Ingestion failed: {error_message}")
            logger.error(traceback.format_exc())
            
            results['status'] = 'FAILED'
            results['error_message'] = error_message
            results['end_time'] = datetime.now()
            
            if load_id:
                self.update_load_metadata(load_id, 'FAILED', 
                                        results.get('records_loaded', 0),
                                        results.get('records_failed', 0),
                                        error_message)
        
        return results

def main():
    """Main function to run the ingestion pipeline."""
    # Initialize pipeline
    pipeline = RetailDataIngestionPipeline()
    
    # Define source file path
    data_file = Path('data/raw/online_retail_ii.parquet')
    
    if not data_file.exists():
        logger.error(f"❌ Data file not found: {data_file}")
        logger.info("💡 Please run the data download script first:")
        logger.info("   python src/ingestion/download_data.py")
        return
    
    # Run ingestion
    results = pipeline.ingest_retail_data(str(data_file))
    
    # Print final summary
    print("\n" + "="*50)
    print("🏁 INGESTION SUMMARY")
    print("="*50)
    print(f"Status: {results['status']}")
    print(f"Load ID: {results['load_id']}")
    print(f"Records Processed: {results['records_processed']:,}")
    print(f"Records Loaded: {results['records_loaded']:,}")
    print(f"Records Failed: {results['records_failed']:,}")
    
    if results['end_time'] and results['start_time']:
        duration = (results['end_time'] - results['start_time']).total_seconds()
        print(f"Duration: {duration:.1f} seconds")
    
    if results['error_message']:
        print(f"Error: {results['error_message']}")
    
    print("="*50)

if __name__ == "__main__":
    main()
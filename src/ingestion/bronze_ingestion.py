"""
Bronze Layer Data Ingestion Pipeline

This module handles raw data ingestion from UCI Online Retail dataset into
the bronze layer with full metadata tracking, lineage information, and
comprehensive error handling.

Features:
- Batch processing with unique batch IDs
- Complete data lineage tracking
- Record-level hashing for duplicate detection
- Ingestion logging and monitoring
- Robust error handling and recovery
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import pandas as pd
import hashlib
import uuid
from datetime import datetime
from pathlib import Path
import logging
from typing import Optional, Tuple
from sqlalchemy import text, create_engine

from src.utils.database import DatabaseManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bronze_ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BronzeIngestionPipeline:
    """
    Handles data ingestion from source files into bronze layer.
    
    This class manages the complete ETL process for bronze layer ingestion,
    including metadata enrichment, batch tracking, and quality logging.
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize bronze ingestion pipeline.
        
        Args:
            db_manager: DatabaseManager instance (creates new if None)
        """
        self.db = db_manager or DatabaseManager()
        self.batch_id = self._generate_batch_id()
        self.ingestion_start_time = datetime.now()
        
    def _generate_batch_id(self) -> str:
        """Generate unique batch ID for this ingestion run."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        return f"BATCH_{timestamp}_{unique_id}"
    
    def _compute_record_hash(self, record: pd.Series) -> str:
        """
        Compute MD5 hash of record for duplicate detection.
        
        Args:
            record: Pandas Series representing a single record
            
        Returns:
            str: MD5 hash of record values
        """
        # Concatenate all source column values
        source_cols = ['invoice', 'stock_code', 'description', 'quantity',
                      'invoice_date', 'unit_price', 'customer_id', 'country']
        
        record_string = '|'.join([str(record.get(col, '')) for col in source_cols])
        return hashlib.md5(record_string.encode()).hexdigest()
    
    def extract_data(self, source_file_path: str) -> Tuple[pd.DataFrame, dict]:
        """
        Extract data from source file.
        
        Args:
            source_file_path: Path to source data file
            
        Returns:
            Tuple of (DataFrame, metadata_dict)
        """
        logger.info(f"📥 Extracting data from: {source_file_path}")
        
        try:
            # Read source file based on extension
            file_path = Path(source_file_path)
            
            if file_path.suffix == '.csv':
                df = pd.read_csv(source_file_path)
            elif file_path.suffix == '.parquet':
                df = pd.read_parquet(source_file_path)
            elif file_path.suffix in ['.xlsx', '.xls']:
                df = pd.read_excel(source_file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
            
            # Collect metadata
            metadata = {
                'source_file_name': file_path.name,
                'source_file_path': str(file_path.absolute()),
                'records_extracted': len(df),
                'file_size_mb': file_path.stat().st_size / (1024 * 1024)
            }
            
            logger.info(f"✅ Extracted {len(df):,} records from {file_path.name}")
            logger.info(f"📊 File size: {metadata['file_size_mb']:.2f} MB")
            logger.info(f"📋 Columns: {list(df.columns)}")
            
            return df, metadata
            
        except Exception as e:
            logger.error(f"❌ Extraction failed: {e}")
            raise
    
    def transform_for_bronze(self, df: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        """
        Transform data for bronze layer ingestion.
        
        Adds metadata columns required for lineage tracking and auditing.
        Minimal transformation applied - data stored as-is.
        
        Args:
            df: Source DataFrame
            metadata: Metadata dictionary from extraction
            
        Returns:
            pd.DataFrame: Transformed DataFrame with metadata columns
        """
        logger.info("🔄 Transforming data for bronze layer...")
        
        try:
            # Create copy to avoid modifying original
            bronze_df = df.copy()
            
            # Rename columns to match bronze table schema (snake_case)
            column_mapping = {
                'InvoiceNo': 'invoice',
                'StockCode': 'stock_code',
                'Description': 'description',
                'Quantity': 'quantity',
                'InvoiceDate': 'invoice_date',
                'UnitPrice': 'unit_price',
                'CustomerID': 'customer_id',
                'Customer ID': 'customer_id',
                'Country': 'country'
            }
            
            # Apply column mapping (case-insensitive)
            for old_col in bronze_df.columns:
                for key, value in column_mapping.items():
                    if old_col.lower() == key.lower():
                        bronze_df.rename(columns={old_col: value}, inplace=True)
                        break
            
            # Convert all source columns to TEXT (preserve original values)
            source_columns = ['invoice', 'stock_code', 'description', 'quantity',
                            'invoice_date', 'unit_price', 'customer_id', 'country']
            
            for col in source_columns:
                if col in bronze_df.columns:
                    bronze_df[col] = bronze_df[col].astype(str)
                else:
                    bronze_df[col] = None
            
            # ================================================================
            # ADD METADATA COLUMNS FOR LINEAGE AND AUDITING
            # ================================================================
            
            # Data lineage
            bronze_df['source_file_name'] = metadata['source_file_name']
            bronze_df['source_file_path'] = metadata['source_file_path']
            bronze_df['source_system'] = 'UCI_ML_REPO'
            
            # Audit information
            bronze_df['load_timestamp'] = datetime.now()
            bronze_df['load_date'] = datetime.now().date()
            bronze_df['load_batch_id'] = self.batch_id
            bronze_df['ingestion_process_id'] = f"BRONZE_INGESTION_{self.batch_id}"
            
            # Record hash for duplicate detection
            bronze_df['record_hash'] = bronze_df.apply(self._compute_record_hash, axis=1)
            
            # Flags
            bronze_df['is_deleted'] = False
            bronze_df['created_by'] = os.getenv('USER', 'system')
            bronze_df['created_at'] = datetime.now()
            
            logger.info(f"✅ Transformation complete: {len(bronze_df):,} records prepared")
            logger.info(f"📊 Added {len([c for c in bronze_df.columns if c not in source_columns])} metadata columns")
            
            return bronze_df
            
        except Exception as e:
            logger.error(f"❌ Transformation failed: {e}")
            raise
    
    def load_to_bronze(self, df: pd.DataFrame, metadata: dict, chunksize: int = 10000) -> int:
        """
        Load transformed data into bronze layer.
        
        Args:
            df: Transformed DataFrame
            metadata: Metadata dictionary
            chunksize: Number of rows per insert batch
            
        Returns:
            int: Number of records loaded
        """
        logger.info(f"⬆️  Loading {len(df):,} records to bronze.online_retail_raw in chunks of {chunksize}...")

        self._log_ingestion_start(metadata)
        total_inserted = 0
        
        try:
            # Split DataFrame into chunks
            for i in range(0, len(df), chunksize):
                chunk = df.iloc[i:i + chunksize]
                inserted = self.db.bulk_insert_dataframe(
                    df=chunk,
                    table_name='online_retail_raw',
                    schema='bronze',
                    if_exists='append',
                    chunksize=chunksize
                )
                total_inserted += inserted
                logger.info(f"Chunk {i // chunksize + 1} inserted ({inserted} records)") 

            self._log_ingestion_success(total_inserted)
            logger.info(f"✅ Successfully loaded {total_inserted:,} records to bronze layer")
            return total_inserted    
                        
        except Exception as e:
            # Log failure
            self._log_ingestion_failure(str(e))
            logger.error(f"❌ Bronze load failed: {e}")
            raise
    
    def _log_ingestion_start(self, metadata: dict):
        """Log ingestion job start in bronze.ingestion_log."""
        log_entry = pd.DataFrame([{
            'batch_id': self.batch_id,
            'source_file_name': metadata['source_file_name'],
            'source_file_path': metadata['source_file_path'],
            'ingestion_start_time': self.ingestion_start_time,
            'status': 'RUNNING',
            'records_processed': metadata['records_extracted'],
            'created_by': os.getenv('USER', 'system')
        }])
        
        self.db.bulk_insert_dataframe(
            df=log_entry,
            table_name='ingestion_log',
            schema='bronze',
            if_exists='append'
        )
    
    def _log_ingestion_success(self, records_inserted: int):
        """Update ingestion log with success status."""
        ingestion_end_time = datetime.now()
        duration = (ingestion_end_time - self.ingestion_start_time).total_seconds()

        update_query = text("""
            UPDATE bronze.ingestion_log
            SET ingestion_end_time = :end_time,
                status = :status,
                records_inserted = :records_inserted,
                ingestion_duration_seconds = :duration
            WHERE batch_id = :batch_id
        """)

        with self.db.get_engine().begin() as conn:
            conn.execute(update_query, {
                "end_time": ingestion_end_time,
                "status": "SUCCESS",
                "records_inserted": records_inserted,
                "duration": duration,
                "batch_id": self.batch_id
            })
    
    def _log_ingestion_failure(self, error_message: str):
        """Update ingestion log with failure status."""
        ingestion_end_time = datetime.now()
        duration = (ingestion_end_time - self.ingestion_start_time).total_seconds()

        update_query = text("""
            UPDATE bronze.ingestion_log
            SET ingestion_end_time = :end_time,
                status = :status,
                records_failed = :records_failed,
                error_message = :error_message,
                ingestion_duration_seconds = :duration
            WHERE batch_id = :batch_id
        """)

        with self.db.get_engine().begin() as conn:
            conn.execute(update_query, {
                "end_time": ingestion_end_time,
                "status": "FAILED",
                "records_failed": None,
                "error_message": error_message,
                "duration": duration,
                "batch_id": self.batch_id
            })
    
    def run_pipeline(self, source_file_path: str, chunksize: int = 10000) -> dict:
        """
        Execute complete bronze ingestion pipeline.
        
        Args:
            source_file_path: Path to source data file
            chunksize: Number of rows per insert batch
            
        Returns:
            dict: Pipeline execution summary
        """
        logger.info("=" * 80)
        logger.info("🚀 BRONZE LAYER INGESTION PIPELINE STARTED")
        logger.info(f"📦 Batch ID: {self.batch_id}")
        logger.info("=" * 80)
        
        try:
            # Extract
            df, metadata = self.extract_data(source_file_path)
            
            # Transform
            bronze_df = self.transform_for_bronze(df, metadata)
            
            # Load in chunks
            records_loaded = self.load_to_bronze(bronze_df, metadata, chunksize=chunksize)
            
            # Pipeline summary
            execution_time = (datetime.now() - self.ingestion_start_time).total_seconds()
            
            summary = {
                'status': 'SUCCESS',
                'batch_id': self.batch_id,
                'records_extracted': len(df),
                'records_loaded': records_loaded,
                'execution_time_seconds': execution_time,
                'records_per_second': records_loaded / execution_time if execution_time > 0 else 0
            }
            
            logger.info("=" * 80)
            logger.info("✅ BRONZE LAYER INGESTION COMPLETED SUCCESSFULLY")
            logger.info(f"📊 Records Loaded: {records_loaded:,}")
            logger.info(f"⏱️  Execution Time: {execution_time:.2f} seconds")
            logger.info(f"🚀 Throughput: {summary['records_per_second']:.0f} records/second")
            logger.info("=" * 80)
            
            return summary
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error("❌ BRONZE LAYER INGESTION FAILED")
            logger.error(f"💥 Error: {e}")
            logger.error("=" * 80)
            raise


def main():
    """Main entry point for bronze ingestion pipeline."""
    # Initialize pipeline
    pipeline = BronzeIngestionPipeline()
    
    # Source file path (adjust as needed)
    source_file = '/home/gaurav/data-engineering-capstone/data/raw/online_retail_ii.parquet'
    
    # Run pipeline
    try:
        summary = pipeline.run_pipeline(source_file)
        print(f"\n🎉 Pipeline Summary: {summary}")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
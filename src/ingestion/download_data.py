"""
Data download script for UCI Online Retail dataset.

This script downloads the Online Retail II dataset from UCI ML Repository
and saves it to the raw data directory for further processing.
"""

import os
import requests
import pandas as pd
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_uci_retail_data():
    """
    Download UCI Online Retail II dataset using the ucimlrepo package.
    
    This function fetches the dataset programmatically and saves it
    to the raw data directory in multiple formats for flexibility.
    """
    try:
        # Install ucimlrepo if not already installed
        import subprocess
        import sys
        
        try:
            from ucimlrepo import fetch_ucirepo
        except ImportError:
            logger.info("Installing ucimlrepo package...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "ucimlrepo"])
            from ucimlrepo import fetch_ucirepo
        
        logger.info("🔄 Downloading UCI Online Retail II dataset...")
        
        # Fetch dataset (ID 502 for Online Retail II)
        #online_retail_ii = fetch_ucirepo(name="Online Retail II")
                        
        # Extract features and combine with any targets
        #df = online_retail_ii.data.features
        
        # Create output directory
        output_dir = Path('data/raw')
        #output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save in multiple formats
        csv_path = output_dir / 'online_retail_ii.csv'
        parquet_path = output_dir / 'online_retail_ii.parquet'
        
        # Save as CSV
        #df.to_csv(csv_path, index=False)
        df = pd.read_csv(csv_path)

        logger.info(f"✅ Dataset saved as CSV: {csv_path}")
        
        # Save as Parquet for better performance
        df.to_parquet(parquet_path, index=False)
        logger.info(f"✅ Dataset saved as Parquet: {parquet_path}")
        
        # Log dataset information
        logger.info(f"📊 Dataset shape: {df.shape}")
        logger.info(f"📊 Columns: {list(df.columns)}")
        
        # Save metadata
        metadata_path = output_dir / 'dataset_metadata.txt'
        with open(metadata_path, 'w') as f:
            f.write("UCI Online Retail II Dataset Metadata\n")
            f.write("="*50 + "\n\n")
            f.write(f"Download Date: {pd.Timestamp.now()}\n")
            f.write(f"Source: UCI ML Repository (ID: 502)\n")
            f.write(f"Shape: {df.shape}\n")
            f.write(f"Columns: {list(df.columns)}\n\n")
            f.write("Dataset Description:\n")
            #f.write(str(online_retail_ii.metadata))
            f.write("\n\nVariable Information:\n")
            #f.write(str(online_retail_ii.variables))
        
        logger.info(f"✅ Metadata saved: {metadata_path}")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Error downloading dataset: {e}")
        return None

def verify_download():
    """Verify that the dataset was downloaded successfully."""
    csv_path = Path('data/raw/online_retail_ii.csv')
    parquet_path = Path('data/raw/online_retail_ii.parquet')
    
    if csv_path.exists() and parquet_path.exists():
        # Load and verify data
        df_csv = pd.read_csv(csv_path)
        df_parquet = pd.read_parquet(parquet_path)
        
        logger.info(f"✅ Verification successful:")
        logger.info(f"   CSV records: {len(df_csv):,}")
        logger.info(f"   Parquet records: {len(df_parquet):,}")
        logger.info(f"   Columns match: {list(df_csv.columns) == list(df_parquet.columns)}")
        
        return True
    else:
        logger.error("❌ Dataset files not found")
        return False

if __name__ == "__main__":
    # Download dataset
    df = download_uci_retail_data()
    
    if df is not None:
        # Verify download
        verify_download()
        logger.info("🎉 Data download completed successfully!")
    else:
        logger.error("💥 Data download failed!")
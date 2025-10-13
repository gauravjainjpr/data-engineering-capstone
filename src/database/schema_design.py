"""
Schema design and management for Medallion Architecture.

This module defines the database schemas for Bronze, Silver, and Gold layers
following data warehouse best practices and star schema design principles.
"""

import os
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from src.utils.database import DatabaseManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SchemaManager:
    """
    Manages database schema creation and maintenance for the Medallion Architecture.
    
    This class handles the creation of Bronze, Silver, and Gold schemas,
    along with their associated tables, constraints, and indexes.
    """
    
    def __init__(self, environment='development'):
        """Initialize schema manager with database connection."""
        self.db_manager = DatabaseManager(environment)
        self.engine = self.db_manager.get_engine()
        
    def create_medallion_schemas(self):
        """
        Create the three main schemas for Medallion Architecture.
        
        Bronze: Raw data storage with minimal processing
        Silver: Cleaned and validated data with business rules applied
        Gold: Aggregated, business-ready data optimized for analytics
        """
        schemas = ['bronze', 'silver', 'gold']
        
        try:
            with self.engine.connect() as conn:
                for schema in schemas:
                    logger.info(f"🔄 Creating {schema} schema...")
                    
                    # Create schema if it doesn't exist
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                    
                    # Grant permissions to de_user
                    conn.execute(text(f"""
                        GRANT USAGE ON SCHEMA {schema} TO de_user;
                        GRANT CREATE ON SCHEMA {schema} TO de_user;
                        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} TO de_user;
                        GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {schema} TO de_user;
                        
                        -- Grant future privileges
                        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} 
                        GRANT ALL PRIVILEGES ON TABLES TO de_user;
                        ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} 
                        GRANT ALL PRIVILEGES ON SEQUENCES TO de_user;
                    """))
                    
                    logger.info(f"✅ {schema.capitalize()} schema created successfully")
                
                # Commit all changes
                conn.commit()
                logger.info("🎉 All Medallion Architecture schemas created!")
                
        except SQLAlchemyError as e:
            logger.error(f"❌ Error creating schemas: {e}")
            raise
    
    def create_bronze_tables(self):
        """
        Create Bronze layer tables for raw data storage.
        
        Bronze tables store data in its original form with minimal transformation.
        They include audit columns for tracking data lineage and load times.
        """
        logger.info("🔄 Creating Bronze layer tables...")
        
        # Bronze table for raw online retail data
        bronze_retail_raw_sql = """
        CREATE TABLE IF NOT EXISTS bronze.retail_raw (
            -- Raw data columns (exactly as received from source)
            invoice VARCHAR(20),
            stock_code VARCHAR(20),
            description TEXT,
            quantity INTEGER,
            invoice_date TIMESTAMP,
            unit_price DECIMAL(10,3),
            customer_id VARCHAR(20),
            country VARCHAR(100),
            
            -- Audit columns for data lineage
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_file VARCHAR(255),
            record_hash VARCHAR(64),
            load_id UUID DEFAULT gen_random_uuid(),
            
            -- Primary key and indexes
            CONSTRAINT pk_bronze_retail_raw PRIMARY KEY (load_id)
        );
        
        -- Create indexes for common query patterns
        CREATE INDEX IF NOT EXISTS idx_bronze_retail_invoice_date 
            ON bronze.retail_raw (invoice_date);
        CREATE INDEX IF NOT EXISTS idx_bronze_retail_customer_id 
            ON bronze.retail_raw (customer_id);
        CREATE INDEX IF NOT EXISTS idx_bronze_retail_stock_code 
            ON bronze.retail_raw (stock_code);
        CREATE INDEX IF NOT EXISTS idx_bronze_retail_load_timestamp 
            ON bronze.retail_raw (load_timestamp);
        """
        
        # Bronze metadata table for tracking data loads
        bronze_load_metadata_sql = """
        CREATE TABLE IF NOT EXISTS bronze.load_metadata (
            load_id UUID DEFAULT gen_random_uuid(),
            source_name VARCHAR(100) NOT NULL,
            file_path VARCHAR(500),
            load_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            load_end_time TIMESTAMP,
            records_loaded INTEGER,
            records_failed INTEGER,
            load_status VARCHAR(20) DEFAULT 'STARTED',
            error_message TEXT,
            created_by VARCHAR(100) DEFAULT CURRENT_USER,
            
            CONSTRAINT pk_bronze_load_metadata PRIMARY KEY (load_id),
            CONSTRAINT chk_load_status CHECK (load_status IN ('STARTED', 'COMPLETED', 'FAILED'))
        );
        
        CREATE INDEX IF NOT EXISTS idx_bronze_load_metadata_source 
            ON bronze.load_metadata (source_name, load_start_time);
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(bronze_retail_raw_sql))
                conn.execute(text(bronze_load_metadata_sql))
                conn.commit()
                logger.info("✅ Bronze layer tables created successfully")
                
        except SQLAlchemyError as e:
            logger.error(f"❌ Error creating Bronze tables: {e}")
            raise
    
    def create_silver_tables(self):
        """
        Create Silver layer tables for cleaned and validated data.
        
        Silver tables contain business rules, data quality checks,
        and standardized formats ready for dimensional modeling.
        """
        logger.info("🔄 Creating Silver layer tables...")
        
        # Silver cleaned retail data with business rules
        silver_retail_cleaned_sql = """
        CREATE TABLE IF NOT EXISTS silver.retail_cleaned (
            -- Cleaned and standardized data columns
            transaction_id UUID DEFAULT gen_random_uuid(),
            invoice_number VARCHAR(20) NOT NULL,
            stock_code VARCHAR(20) NOT NULL,
            description TEXT,
            quantity INTEGER NOT NULL,
            invoice_date TIMESTAMP NOT NULL,
            unit_price DECIMAL(10,3) NOT NULL,
            customer_id VARCHAR(20),
            country VARCHAR(100) NOT NULL,
            
            -- Derived business columns
            revenue DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
            is_return BOOLEAN GENERATED ALWAYS AS (quantity < 0) STORED,
            transaction_type VARCHAR(10) GENERATED ALWAYS AS (
                CASE WHEN quantity < 0 THEN 'RETURN' ELSE 'SALE' END
            ) STORED,
            
            -- Data quality flags
            has_customer_id BOOLEAN GENERATED ALWAYS AS (customer_id IS NOT NULL) STORED,
            is_valid_price BOOLEAN GENERATED ALWAYS AS (unit_price > 0) STORED,
            is_valid_quantity BOOLEAN GENERATED ALWAYS AS (quantity != 0) STORED,
            
            -- Audit columns
            source_load_id UUID NOT NULL,
            processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data_quality_score DECIMAL(3,2),
            
            -- Constraints and business rules
            CONSTRAINT pk_silver_retail_cleaned PRIMARY KEY (transaction_id),
            CONSTRAINT fk_silver_source_load FOREIGN KEY (source_load_id) 
                REFERENCES bronze.load_metadata(load_id),
            CONSTRAINT chk_valid_price CHECK (unit_price > 0),
            CONSTRAINT chk_non_zero_quantity CHECK (quantity != 0),
            CONSTRAINT chk_valid_date CHECK (invoice_date >= '2009-01-01'),
            CONSTRAINT chk_quality_score CHECK (data_quality_score BETWEEN 0 AND 1)
        );
        
        -- Create indexes for analytics queries
        CREATE INDEX IF NOT EXISTS idx_silver_retail_invoice_date 
            ON silver.retail_cleaned (invoice_date);
        CREATE INDEX IF NOT EXISTS idx_silver_retail_customer_country 
            ON silver.retail_cleaned (customer_id, country);
        CREATE INDEX IF NOT EXISTS idx_silver_retail_stock_code 
            ON silver.retail_cleaned (stock_code);
        CREATE INDEX IF NOT EXISTS idx_silver_retail_transaction_type 
            ON silver.retail_cleaned (transaction_type, invoice_date);
        """
        
        # Silver data quality metrics table
        silver_quality_metrics_sql = """
        CREATE TABLE IF NOT EXISTS silver.data_quality_metrics (
            metric_id UUID DEFAULT gen_random_uuid(),
            load_id UUID NOT NULL,
            table_name VARCHAR(100) NOT NULL,
            metric_name VARCHAR(100) NOT NULL,
            metric_value DECIMAL(15,4),
            metric_description TEXT,
            measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT pk_silver_quality_metrics PRIMARY KEY (metric_id),
            CONSTRAINT fk_quality_load FOREIGN KEY (load_id) 
                REFERENCES bronze.load_metadata(load_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_silver_quality_table_metric 
            ON silver.data_quality_metrics (table_name, metric_name, measured_at);
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(silver_retail_cleaned_sql))
                conn.execute(text(silver_quality_metrics_sql))
                conn.commit()
                logger.info("✅ Silver layer tables created successfully")
                
        except SQLAlchemyError as e:
            logger.error(f"❌ Error creating Silver tables: {e}")
            raise
    
    def create_gold_star_schema(self):
        """
        Create Gold layer tables using Star Schema design for analytics.
        
        Star schema consists of fact tables (metrics) surrounded by
        dimension tables (descriptive attributes) for optimal query performance.
        """
        logger.info("🔄 Creating Gold layer Star Schema...")
        
        # Dimension Tables
        
        # Date Dimension - Essential for time-based analytics
        dim_date_sql = """
        CREATE TABLE IF NOT EXISTS gold.dim_date (
            date_key INTEGER PRIMARY KEY,
            full_date DATE NOT NULL UNIQUE,
            day_of_week INTEGER NOT NULL,
            day_name VARCHAR(10) NOT NULL,
            day_of_month INTEGER NOT NULL,
            day_of_year INTEGER NOT NULL,
            week_of_year INTEGER NOT NULL,
            month_number INTEGER NOT NULL,
            month_name VARCHAR(10) NOT NULL,
            quarter INTEGER NOT NULL,
            quarter_name VARCHAR(2) NOT NULL,
            year INTEGER NOT NULL,
            is_weekend BOOLEAN NOT NULL,
            is_holiday BOOLEAN DEFAULT FALSE,
            fiscal_year INTEGER,
            fiscal_quarter INTEGER,
            
            CONSTRAINT chk_day_of_week CHECK (day_of_week BETWEEN 1 AND 7),
            CONSTRAINT chk_month CHECK (month_number BETWEEN 1 AND 12),
            CONSTRAINT chk_quarter CHECK (quarter BETWEEN 1 AND 4)
        );
        
        CREATE INDEX IF NOT EXISTS idx_dim_date_year_month 
            ON gold.dim_date (year, month_number);
        CREATE INDEX IF NOT EXISTS idx_dim_date_quarter 
            ON gold.dim_date (year, quarter);
        """
        
        # Product Dimension
        dim_product_sql = """
        CREATE TABLE IF NOT EXISTS gold.dim_product (
            product_key SERIAL PRIMARY KEY,
            stock_code VARCHAR(20) NOT NULL UNIQUE,
            description TEXT,
            product_category VARCHAR(100),
            product_subcategory VARCHAR(100),
            is_gift BOOLEAN DEFAULT FALSE,
            is_set BOOLEAN DEFAULT FALSE,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_current BOOLEAN DEFAULT TRUE,
            
            -- SCD Type 2 columns for historical tracking
            effective_start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            effective_end_date TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_dim_product_category 
            ON gold.dim_product (product_category, product_subcategory);
        CREATE INDEX IF NOT EXISTS idx_dim_product_current 
            ON gold.dim_product (stock_code, is_current);
        """
        
        # Customer Dimension
        dim_customer_sql = """
        CREATE TABLE IF NOT EXISTS gold.dim_customer (
            customer_key SERIAL PRIMARY KEY,
            customer_id VARCHAR(20) UNIQUE,
            country VARCHAR(100) NOT NULL,
            region VARCHAR(50),
            customer_segment VARCHAR(50),
            first_purchase_date DATE,
            last_purchase_date DATE,
            total_orders INTEGER DEFAULT 0,
            total_revenue DECIMAL(12,2) DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Handle unknown customers
            CONSTRAINT chk_customer_id_or_unknown CHECK (
                customer_id IS NOT NULL OR customer_id = 'UNKNOWN'
            )
        );
        
        CREATE INDEX IF NOT EXISTS idx_dim_customer_country 
            ON gold.dim_customer (country, region);
        CREATE INDEX IF NOT EXISTS idx_dim_customer_segment 
            ON gold.dim_customer (customer_segment, is_active);
        """
        
        # Geography Dimension
        dim_geography_sql = """
        CREATE TABLE IF NOT EXISTS gold.dim_geography (
            geography_key SERIAL PRIMARY KEY,
            country VARCHAR(100) NOT NULL UNIQUE,
            region VARCHAR(50),
            continent VARCHAR(50),
            country_code VARCHAR(3),
            time_zone VARCHAR(50),
            currency VARCHAR(3),
            is_eu_member BOOLEAN DEFAULT FALSE,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_dim_geography_region 
            ON gold.dim_geography (region, continent);
        """
        
        # Fact Table - Sales Transactions
        fact_sales_sql = """
        CREATE TABLE IF NOT EXISTS gold.fact_sales (
            -- Surrogate key
            sales_key BIGSERIAL PRIMARY KEY,
            
            -- Foreign keys to dimensions (Star Schema)
            date_key INTEGER NOT NULL,
            product_key INTEGER NOT NULL,
            customer_key INTEGER NOT NULL,
            geography_key INTEGER NOT NULL,
            
            -- Degenerate dimensions (stored in fact table)
            invoice_number VARCHAR(20) NOT NULL,
            transaction_id UUID NOT NULL,
            
            -- Measures (additive facts)
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(10,3) NOT NULL,
            revenue DECIMAL(12,2) NOT NULL,
            discount_amount DECIMAL(12,2) DEFAULT 0,
            net_revenue DECIMAL(12,2) GENERATED ALWAYS AS (revenue - discount_amount) STORED,
            
            -- Semi-additive facts
            running_total_revenue DECIMAL(15,2),
            
            -- Non-additive facts
            unit_cost DECIMAL(10,3),
            profit_margin DECIMAL(5,4),
            
            -- Flags and indicators
            is_return BOOLEAN NOT NULL,
            transaction_type VARCHAR(10) NOT NULL,
            
            -- Audit columns
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_load_id UUID NOT NULL,
            
            -- Foreign key constraints
            CONSTRAINT fk_fact_sales_date FOREIGN KEY (date_key) 
                REFERENCES gold.dim_date(date_key),
            CONSTRAINT fk_fact_sales_product FOREIGN KEY (product_key) 
                REFERENCES gold.dim_product(product_key),
            CONSTRAINT fk_fact_sales_customer FOREIGN KEY (customer_key) 
                REFERENCES gold.dim_customer(customer_key),
            CONSTRAINT fk_fact_sales_geography FOREIGN KEY (geography_key) 
                REFERENCES gold.dim_geography(geography_key),
            CONSTRAINT fk_fact_sales_load FOREIGN KEY (source_load_id) 
                REFERENCES bronze.load_metadata(load_id),
                
            -- Business constraints
            CONSTRAINT chk_fact_sales_quantity CHECK (quantity != 0),
            CONSTRAINT chk_fact_sales_price CHECK (unit_price > 0),
            CONSTRAINT chk_fact_sales_type CHECK (transaction_type IN ('SALE', 'RETURN'))
        );
        
        -- Partitioning by date for performance (optional but recommended)
        -- ALTER TABLE gold.fact_sales PARTITION BY RANGE (date_key);
        
        -- Create indexes for common query patterns
        CREATE INDEX IF NOT EXISTS idx_fact_sales_date 
            ON gold.fact_sales (date_key);
        CREATE INDEX IF NOT EXISTS idx_fact_sales_product_date 
            ON gold.fact_sales (product_key, date_key);
        CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_date 
            ON gold.fact_sales (customer_key, date_key);
        CREATE INDEX IF NOT EXISTS idx_fact_sales_geography_date 
            ON gold.fact_sales (geography_key, date_key);
        CREATE INDEX IF NOT EXISTS idx_fact_sales_invoice 
            ON gold.fact_sales (invoice_number);
        """
        
        try:
            with self.engine.connect() as conn:
                # Create dimension tables first
                conn.execute(text(dim_date_sql))
                conn.execute(text(dim_product_sql))
                conn.execute(text(dim_customer_sql))
                conn.execute(text(dim_geography_sql))
                
                # Create fact table last (due to foreign key dependencies)
                conn.execute(text(fact_sales_sql))
                
                conn.commit()
                logger.info("✅ Gold layer Star Schema created successfully")
                
        except SQLAlchemyError as e:
            logger.error(f"❌ Error creating Gold Star Schema: {e}")
            raise
    
    def populate_date_dimension(self, start_date='2009-01-01', end_date='2012-12-31'):
        """
        Populate the date dimension with a range of dates.
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
        """
        logger.info(f"🔄 Populating date dimension from {start_date} to {end_date}...")
        
        populate_date_sql = f"""
        INSERT INTO gold.dim_date (
            date_key, full_date, day_of_week, day_name, day_of_month, 
            day_of_year, week_of_year, month_number, month_name, 
            quarter, quarter_name, year, is_weekend, fiscal_year, fiscal_quarter
        )
        SELECT 
            TO_CHAR(date_series, 'YYYYMMDD')::INTEGER as date_key,
            date_series::DATE as full_date,
            EXTRACT(DOW FROM date_series)::INTEGER + 1 as day_of_week,
            TO_CHAR(date_series, 'Day')::VARCHAR(10) as day_name,
            EXTRACT(DAY FROM date_series)::INTEGER as day_of_month,
            EXTRACT(DOY FROM date_series)::INTEGER as day_of_year,
            EXTRACT(WEEK FROM date_series)::INTEGER as week_of_year,
            EXTRACT(MONTH FROM date_series)::INTEGER as month_number,
            TO_CHAR(date_series, 'Month')::VARCHAR(10) as month_name,
            EXTRACT(QUARTER FROM date_series)::INTEGER as quarter,
            'Q' || EXTRACT(QUARTER FROM date_series)::VARCHAR(2) as quarter_name,
            EXTRACT(YEAR FROM date_series)::INTEGER as year,
            CASE WHEN EXTRACT(DOW FROM date_series) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend,
            CASE 
                WHEN EXTRACT(MONTH FROM date_series) <= 3 THEN EXTRACT(YEAR FROM date_series)::INTEGER
                ELSE EXTRACT(YEAR FROM date_series)::INTEGER + 1 
            END as fiscal_year,
            CASE 
                WHEN EXTRACT(MONTH FROM date_series) <= 3 THEN EXTRACT(QUARTER FROM date_series)::INTEGER + 1
                WHEN EXTRACT(MONTH FROM date_series) <= 6 THEN 1
                WHEN EXTRACT(MONTH FROM date_series) <= 9 THEN 2
                ELSE 3
            END as fiscal_quarter
        FROM generate_series('{start_date}'::DATE, '{end_date}'::DATE, '1 day'::INTERVAL) as date_series
        ON CONFLICT (date_key) DO NOTHING;
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(populate_date_sql))
                conn.commit()
                logger.info(f"✅ Date dimension populated with dates from {start_date} to {end_date}")
                
        except SQLAlchemyError as e:
            logger.error(f"❌ Error populating date dimension: {e}")
            raise
    
    def create_all_schemas(self):
        """Create all schemas and tables for the Medallion Architecture."""
        logger.info("🚀 Creating complete Medallion Architecture...")
        
        try:
            # Create schemas
            self.create_medallion_schemas()
            
            # Create tables in order (Bronze -> Silver -> Gold)
            self.create_bronze_tables()
            self.create_silver_tables()
            self.create_gold_star_schema()
            
            # Populate reference data
            self.populate_date_dimension()
            
            logger.info("🎉 Medallion Architecture created successfully!")
            logger.info("📋 Summary:")
            logger.info("   ✅ Bronze schema: Raw data storage")
            logger.info("   ✅ Silver schema: Cleaned data with business rules")
            logger.info("   ✅ Gold schema: Star schema for analytics")
            logger.info("   ✅ Date dimension populated (2009-2012)")
            
        except Exception as e:
            logger.error(f"💥 Failed to create Medallion Architecture: {e}")
            raise

if __name__ == "__main__":
    # Create the complete architecture
    schema_manager = SchemaManager()
    schema_manager.create_all_schemas()
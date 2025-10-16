-- ============================================================================
-- BRONZE LAYER TABLE DEFINITIONS
-- ============================================================================
-- These tables store raw data from the UCI Online Retail dataset with
-- additional metadata columns for data lineage, auditing, and traceability.
--
-- Key Design Principles:
-- 1. Store data exactly as received from source (minimal transformation)
-- 2. Add metadata columns for lineage tracking
-- 3. Use TEXT data types to prevent data loss from type mismatches
-- 4. Never delete or update bronze data (append-only)
-- 5. Include source file information for debugging
-- ============================================================================

-- ============================================================================
-- bronze.online_retail_raw
-- ============================================================================
-- Raw ingestion table for UCI Online Retail II dataset
-- This table mirrors the source CSV structure with added metadata columns
-- ============================================================================

CREATE TABLE bronze.online_retail_raw (
    -- Surrogate key (auto-generated unique identifier)
    bronze_id BIGSERIAL PRIMARY KEY,
    
    -- Source data columns (kept as TEXT to preserve original values)
    invoice TEXT,
    stock_code TEXT,
    description TEXT,
    quantity TEXT,
    invoice_date TEXT,
    unit_price TEXT,
    customer_id TEXT,
    country TEXT,
    
    -- ========================================================================
    -- METADATA COLUMNS FOR LINEAGE AND AUDITING
    -- ========================================================================
    
    -- Data lineage: where did this record come from?
    source_file_name TEXT NOT NULL,          -- Original filename
    source_file_path TEXT,                    -- Full file path
    source_system TEXT DEFAULT 'UCI_ML_REPO', -- Source system identifier
    
    -- Audit: when and how was this record loaded?
    load_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    load_date DATE DEFAULT CURRENT_DATE NOT NULL,
    load_batch_id TEXT,                       -- Unique batch identifier
    ingestion_process_id TEXT,                -- Process/job that loaded this record
    
    -- Data quality: record-level metadata
    record_hash TEXT,                         -- Hash of source data for duplicate detection
    is_deleted BOOLEAN DEFAULT FALSE,         -- Soft delete flag (never actually delete)
    
    -- Additional metadata
    created_by TEXT DEFAULT CURRENT_USER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Create indexes for query performance
CREATE INDEX idx_bronze_online_retail_load_timestamp 
    ON bronze.online_retail_raw(load_timestamp);

CREATE INDEX idx_bronze_online_retail_load_batch_id 
    ON bronze.online_retail_raw(load_batch_id);

CREATE INDEX idx_bronze_online_retail_invoice 
    ON bronze.online_retail_raw(invoice);

CREATE INDEX idx_bronze_online_retail_customer_id 
    ON bronze.online_retail_raw(customer_id);

-- Create unique constraint on record hash to prevent exact duplicates within same batch
CREATE INDEX idx_bronze_online_retail_record_hash 
    ON bronze.online_retail_raw(record_hash);

-- Add table comment
COMMENT ON TABLE bronze.online_retail_raw IS 
'Bronze layer raw data from UCI Online Retail II dataset. Contains unprocessed source data with lineage metadata for auditing and traceability. Data is append-only and immutable.';

-- Add column comments for documentation
COMMENT ON COLUMN bronze.online_retail_raw.bronze_id IS 'Surrogate key for unique record identification';
COMMENT ON COLUMN bronze.online_retail_raw.source_file_name IS 'Source filename for data lineage tracking';
COMMENT ON COLUMN bronze.online_retail_raw.load_timestamp IS 'Exact timestamp when record was loaded';
COMMENT ON COLUMN bronze.online_retail_raw.load_batch_id IS 'Unique identifier for batch loading process';
COMMENT ON COLUMN bronze.online_retail_raw.record_hash IS 'MD5 hash of source data for duplicate detection';

-- ============================================================================
-- bronze.ingestion_log
-- ============================================================================
-- Tracks all ingestion jobs for monitoring and debugging
-- ============================================================================

CREATE TABLE bronze.ingestion_log (
    log_id BIGSERIAL PRIMARY KEY,
    batch_id TEXT UNIQUE NOT NULL,
    source_file_name TEXT NOT NULL,
    source_file_path TEXT,
    ingestion_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    ingestion_end_time TIMESTAMP WITH TIME ZONE,
    status TEXT CHECK (status IN ('STARTED', 'SUCCESS', 'FAILED', 'RUNNING')),
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    ingestion_duration_seconds NUMERIC,
    created_by TEXT DEFAULT CURRENT_USER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_bronze_ingestion_log_batch_id 
    ON bronze.ingestion_log(batch_id);

CREATE INDEX idx_bronze_ingestion_log_status 
    ON bronze.ingestion_log(status);

COMMENT ON TABLE bronze.ingestion_log IS 
'Ingestion job tracking table for monitoring ETL pipeline executions, error handling, and performance analysis.';
-- ============================================================================
-- SILVER LAYER TABLE DEFINITIONS
-- ============================================================================
-- These tables contain cleaned, validated, and standardized data from bronze.
-- Data quality rules are enforced through constraints, and proper data types
-- are applied. Duplicates are removed, and business rules are implemented.
-- ============================================================================

-- ============================================================================
-- silver.retail_transactions
-- ============================================================================
-- Cleaned and validated transaction data with proper data types and constraints
-- ============================================================================

CREATE TABLE silver.retail_transactions (
    -- Surrogate key
    transaction_id BIGSERIAL PRIMARY KEY,
    
    -- Business keys and transaction data (properly typed)
    invoice VARCHAR(50) NOT NULL,
    invoice_line_number INTEGER,  -- Line number within invoice
    stock_code VARCHAR(50) NOT NULL,
    description TEXT,
    quantity INTEGER NOT NULL,
    invoice_date TIMESTAMP NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    customer_id VARCHAR(50),  -- Can be null (some transactions don't have customer)
    country VARCHAR(100) NOT NULL,
    
    -- Calculated fields
    line_total NUMERIC(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    is_return BOOLEAN GENERATED ALWAYS AS (quantity < 0) STORED,
    is_cancelled BOOLEAN DEFAULT FALSE,
    
    -- ========================================================================
    -- DATA QUALITY FLAGS
    -- ========================================================================
    has_customer_id BOOLEAN GENERATED ALWAYS AS (customer_id IS NOT NULL) STORED,
    has_valid_description BOOLEAN GENERATED ALWAYS AS (description IS NOT NULL AND LENGTH(description) > 0) STORED,
    
    -- ========================================================================
    -- LINEAGE: Reference back to bronze layer
    -- ========================================================================
    bronze_id BIGINT NOT NULL,  -- References bronze.online_retail_raw
    source_batch_id TEXT NOT NULL,
    
    -- ========================================================================
    -- AUDIT COLUMNS
    -- ========================================================================
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    is_current BOOLEAN DEFAULT TRUE,  -- For SCD Type 2 if needed
    
    -- ========================================================================
    -- DATA QUALITY CONSTRAINTS
    -- ========================================================================
    CONSTRAINT chk_quantity_not_zero CHECK (quantity != 0),
    CONSTRAINT chk_unit_price_not_negative CHECK (unit_price >= 0),
    CONSTRAINT chk_invoice_not_empty CHECK (LENGTH(invoice) > 0),
    CONSTRAINT chk_stock_code_not_empty CHECK (LENGTH(stock_code) > 0),
    CONSTRAINT chk_country_not_empty CHECK (LENGTH(country) > 0)
);

-- Create indexes for performance
CREATE INDEX idx_silver_transactions_invoice 
    ON silver.retail_transactions(invoice);

CREATE INDEX idx_silver_transactions_customer_id 
    ON silver.retail_transactions(customer_id);

CREATE INDEX idx_silver_transactions_stock_code 
    ON silver.retail_transactions(stock_code);

CREATE INDEX idx_silver_transactions_invoice_date 
    ON silver.retail_transactions(invoice_date);

CREATE INDEX idx_silver_transactions_country 
    ON silver.retail_transactions(country);

CREATE INDEX idx_silver_transactions_bronze_id 
    ON silver.retail_transactions(bronze_id);

-- Composite index for common queries
CREATE INDEX idx_silver_transactions_date_customer 
    ON silver.retail_transactions(invoice_date, customer_id);

-- Add table and column comments
COMMENT ON TABLE silver.retail_transactions IS 
'Silver layer cleaned transaction data. All data quality rules applied, proper data types enforced, duplicates removed. Ready for analysis and gold layer modeling.';

COMMENT ON COLUMN silver.retail_transactions.line_total IS 
'Calculated field: quantity × unit_price';

COMMENT ON COLUMN silver.retail_transactions.is_return IS 
'Automatically set to TRUE when quantity is negative (returns/cancellations)';

COMMENT ON COLUMN silver.retail_transactions.bronze_id IS 
'Foreign key reference to bronze.online_retail_raw for lineage tracking';

-- ============================================================================
-- silver.data_quality_checks
-- ============================================================================
-- Tracks data quality metrics and validation results
-- ============================================================================

CREATE TABLE silver.data_quality_checks (
    check_id BIGSERIAL PRIMARY KEY,
    batch_id TEXT NOT NULL,
    check_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    check_type VARCHAR(100) NOT NULL,  -- e.g., 'NULL_CHECK', 'DUPLICATE_CHECK', 'CONSTRAINT_VALIDATION'
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    check_passed BOOLEAN NOT NULL,
    records_checked INTEGER,
    records_failed INTEGER,
    failure_percentage NUMERIC(5, 2),
    error_details TEXT,
    created_by TEXT DEFAULT CURRENT_USER
);

CREATE INDEX idx_silver_dq_batch_id 
    ON silver.data_quality_checks(batch_id);

CREATE INDEX idx_silver_dq_check_type 
    ON silver.data_quality_checks(check_type);

COMMENT ON TABLE silver.data_quality_checks IS 
'Data quality validation tracking table. Records all quality checks performed during bronze-to-silver transformation.';
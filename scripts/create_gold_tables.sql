-- ============================================================================
-- GOLD LAYER TABLE DEFINITIONS - STAR SCHEMA
-- ============================================================================
-- Dimensional model optimized for analytics and business intelligence.
-- Follows Kimball methodology with fact and dimension tables.
--
-- Star Schema Components:
-- - Fact Table: fact_sales (transactional metrics)
-- - Dimension Tables: dim_date, dim_product, dim_customer, dim_geography
-- ============================================================================

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- ============================================================================
-- gold.dim_date
-- ============================================================================
-- Date dimension with calendar attributes for time-based analysis
-- ============================================================================

CREATE TABLE gold.dim_date (
    date_key INTEGER PRIMARY KEY,  -- Format: YYYYMMDD (e.g., 20091201)
    full_date DATE UNIQUE NOT NULL,
    
    -- Calendar attributes
    day_of_week INTEGER NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    day_of_week_name VARCHAR(10) NOT NULL,
    day_of_month INTEGER NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    day_of_year INTEGER NOT NULL CHECK (day_of_year BETWEEN 1 AND 366),
    
    week_of_year INTEGER NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    iso_week VARCHAR(10) NOT NULL,  -- Format: YYYY-WNN
    
    month_number INTEGER NOT NULL CHECK (month_number BETWEEN 1 AND 12),
    month_name VARCHAR(10) NOT NULL,
    month_abbr VARCHAR(3) NOT NULL,
    
    quarter_number INTEGER NOT NULL CHECK (quarter_number BETWEEN 1 AND 4),
    quarter_name VARCHAR(10) NOT NULL,  -- Format: Q1, Q2, Q3, Q4
    
    year_number INTEGER NOT NULL,
    
    -- Business calendar attributes
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(100),
    
    -- Fiscal period (assuming fiscal year = calendar year)
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER NOT NULL,
    fiscal_month INTEGER NOT NULL,
    
    -- Relative date attributes (for dynamic filtering)
    is_current_day BOOLEAN DEFAULT FALSE,
    is_current_month BOOLEAN DEFAULT FALSE,
    is_current_quarter BOOLEAN DEFAULT FALSE,
    is_current_year BOOLEAN DEFAULT FALSE,
    
    -- Audit columns
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_gold_dim_date_full_date ON gold.dim_date(full_date);
CREATE INDEX idx_gold_dim_date_year_month ON gold.dim_date(year_number, month_number);
CREATE INDEX idx_gold_dim_date_quarter ON gold.dim_date(year_number, quarter_number);

COMMENT ON TABLE gold.dim_date IS 
'Date dimension table providing calendar and business date attributes for time-based analysis.';

-- ============================================================================
-- gold.dim_product
-- ============================================================================
-- Product dimension with SCD Type 2 for tracking historical changes
-- ============================================================================

CREATE TABLE gold.dim_product (
    product_key BIGSERIAL PRIMARY KEY,  -- Surrogate key
    stock_code VARCHAR(50) NOT NULL,    -- Natural key (business key)
    
    -- Product attributes
    description TEXT NOT NULL,
    product_category VARCHAR(100),  -- Derived from description patterns
    product_type VARCHAR(50),       -- e.g., 'GIFT', 'HOMEWARE', 'SEASONAL'
    
    -- ========================================================================
    -- SCD TYPE 2 COLUMNS: Track historical changes to product attributes
    -- ========================================================================
    effective_date DATE NOT NULL,        -- When this version became effective
    expiration_date DATE,                 -- When this version expired (NULL = current)
    is_current BOOLEAN DEFAULT TRUE,      -- TRUE for current version
    version_number INTEGER DEFAULT 1,     -- Version counter
    
    -- Data quality flags
    has_valid_description BOOLEAN NOT NULL,
    description_length INTEGER,
    
    -- Audit columns
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- SCD Type 2 constraint: only one current record per stock_code
    CONSTRAINT uq_gold_product_current 
        UNIQUE (stock_code, is_current) 
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX idx_gold_dim_product_stock_code ON gold.dim_product(stock_code);
CREATE INDEX idx_gold_dim_product_is_current ON gold.dim_product(is_current);
CREATE INDEX idx_gold_dim_product_category ON gold.dim_product(product_category);
CREATE INDEX idx_gold_dim_product_dates ON gold.dim_product(effective_date, expiration_date);

COMMENT ON TABLE gold.dim_product IS 
'Product dimension with SCD Type 2 implementation for tracking historical changes to product attributes.';

COMMENT ON COLUMN gold.dim_product.effective_date IS 
'Start date when this version of the product became active';

COMMENT ON COLUMN gold.dim_product.expiration_date IS 
'End date when this version expired. NULL indicates current active version.';

-- ============================================================================
-- gold.dim_customer
-- ============================================================================
-- Customer dimension with SCD Type 2 for tracking customer changes
-- ============================================================================

CREATE TABLE gold.dim_customer (
    customer_key BIGSERIAL PRIMARY KEY,  -- Surrogate key
    customer_id VARCHAR(50) NOT NULL,    -- Natural key
    
    -- Customer attributes
    country VARCHAR(100) NOT NULL,       -- Customer's country
    region VARCHAR(100),                  -- Geographic region
    customer_segment VARCHAR(50),         -- e.g., 'RETAIL', 'WHOLESALE', 'VIP'
    
    -- Customer behavior metrics (updated periodically)
    first_purchase_date DATE,
    last_purchase_date DATE,
    total_lifetime_value NUMERIC(12, 2),
    total_orders INTEGER DEFAULT 0,
    
    -- ========================================================================
    -- SCD TYPE 2 COLUMNS
    -- ========================================================================
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    version_number INTEGER DEFAULT 1,
    
    -- Audit columns
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_gold_customer_current 
        UNIQUE (customer_id, is_current) 
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX idx_gold_dim_customer_id ON gold.dim_customer(customer_id);
CREATE INDEX idx_gold_dim_customer_is_current ON gold.dim_customer(is_current);
CREATE INDEX idx_gold_dim_customer_country ON gold.dim_customer(country);
CREATE INDEX idx_gold_dim_customer_segment ON gold.dim_customer(customer_segment);

COMMENT ON TABLE gold.dim_customer IS 
'Customer dimension with SCD Type 2 for tracking customer attribute changes over time.';

-- ============================================================================
-- gold.dim_geography
-- ============================================================================
-- Geography dimension for country-level analysis
-- ============================================================================

CREATE TABLE gold.dim_geography (
    geography_key SERIAL PRIMARY KEY,
    country VARCHAR(100) UNIQUE NOT NULL,
    country_code VARCHAR(3),        -- ISO 3166-1 alpha-3
    region VARCHAR(100),             -- e.g., 'Europe', 'Asia', 'Americas'
    sub_region VARCHAR(100),         -- More granular region
    continent VARCHAR(50),
    is_eu_member BOOLEAN DEFAULT FALSE,
    currency_code VARCHAR(3),        -- ISO 4217
    timezone VARCHAR(50),
    
    -- Audit columns
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_gold_dim_geography_country ON gold.dim_geography(country);
CREATE INDEX idx_gold_dim_geography_region ON gold.dim_geography(region);

COMMENT ON TABLE gold.dim_geography IS 
'Geography dimension providing country-level attributes for geographic analysis.';

-- ============================================================================
-- FACT TABLE
-- ============================================================================

-- ============================================================================
-- gold.fact_sales
-- ============================================================================
-- Central fact table containing sales transactions and metrics
-- Granularity: One row per invoice line item
-- ============================================================================

CREATE TABLE gold.fact_sales (
    sale_key BIGSERIAL PRIMARY KEY,  -- Surrogate key for fact table
    
    -- Foreign keys to dimensions (star schema)
    date_key INTEGER NOT NULL REFERENCES gold.dim_date(date_key),
    product_key BIGINT NOT NULL REFERENCES gold.dim_product(product_key),
    customer_key BIGINT REFERENCES gold.dim_customer(customer_key),  -- Nullable
    geography_key INTEGER NOT NULL REFERENCES gold.dim_geography(geography_key),
    
    -- Degenerate dimensions (transaction identifiers stored in fact)
    invoice VARCHAR(50) NOT NULL,
    invoice_line_number INTEGER,
    
    -- ========================================================================
    -- MEASURES (Additive Facts)
    -- ========================================================================
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    line_total NUMERIC(12, 2) NOT NULL,  -- quantity × unit_price
    
    -- Calculated measures
    discount_amount NUMERIC(12, 2) DEFAULT 0,
    net_amount NUMERIC(12, 2),  -- line_total - discount_amount
    
    -- ========================================================================
    -- SEMI-ADDITIVE & NON-ADDITIVE FACTS
    -- ========================================================================
    is_return BOOLEAN NOT NULL,
    is_cancelled BOOLEAN DEFAULT FALSE,
    
    -- ========================================================================
    -- LINEAGE: Reference back to silver layer
    -- ========================================================================
    silver_transaction_id BIGINT NOT NULL,  -- References silver.retail_transactions
    
    -- ========================================================================
    -- AUDIT COLUMNS
    -- ========================================================================
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    
    -- ========================================================================
    -- CONSTRAINTS
    -- ========================================================================
    CONSTRAINT chk_gold_quantity_not_zero CHECK (quantity != 0),
    CONSTRAINT chk_gold_unit_price_positive CHECK (unit_price >= 0),
    CONSTRAINT chk_gold_line_total CHECK (line_total = quantity * unit_price)
);

-- Create indexes for optimal query performance
CREATE INDEX idx_gold_fact_sales_date_key ON gold.fact_sales(date_key);
CREATE INDEX idx_gold_fact_sales_product_key ON gold.fact_sales(product_key);
CREATE INDEX idx_gold_fact_sales_customer_key ON gold.fact_sales(customer_key);
CREATE INDEX idx_gold_fact_sales_geography_key ON gold.fact_sales(geography_key);
CREATE INDEX idx_gold_fact_sales_invoice ON gold.fact_sales(invoice);

-- Composite indexes for common query patterns
CREATE INDEX idx_gold_fact_sales_date_product 
    ON gold.fact_sales(date_key, product_key);

CREATE INDEX idx_gold_fact_sales_date_customer 
    ON gold.fact_sales(date_key, customer_key);

CREATE INDEX idx_gold_fact_sales_date_geography 
    ON gold.fact_sales(date_key, geography_key);

COMMENT ON TABLE gold.fact_sales IS 
'Fact table containing sales transactions with foreign keys to dimension tables. Optimized for analytical queries and BI reporting.';

COMMENT ON COLUMN gold.fact_sales.line_total IS 
'Total amount for line item (quantity × unit_price). Additive measure.';

COMMENT ON COLUMN gold.fact_sales.silver_transaction_id IS 
'Foreign key to silver.retail_transactions for data lineage tracking.';

-- ============================================================================
-- AGGREGATE/SUMMARY TABLES (Optional - for performance optimization)
-- ============================================================================

-- ============================================================================
-- gold.fact_sales_daily
-- ============================================================================
-- Pre-aggregated daily sales for fast dashboard queries
-- ============================================================================

CREATE TABLE gold.fact_sales_daily (
    daily_sale_key BIGSERIAL PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES gold.dim_date(date_key),
    product_key BIGINT NOT NULL REFERENCES gold.dim_product(product_key),
    geography_key INTEGER NOT NULL REFERENCES gold.dim_geography(geography_key),
    
    -- Aggregated measures
    total_quantity INTEGER NOT NULL,
    total_sales_amount NUMERIC(15, 2) NOT NULL,
    total_returns INTEGER DEFAULT 0,
    total_return_amount NUMERIC(15, 2) DEFAULT 0,
    net_sales_amount NUMERIC(15, 2) NOT NULL,
    
    unique_customers INTEGER,
    unique_invoices INTEGER,
    
    -- Audit
    aggregated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_gold_fact_daily 
        UNIQUE (date_key, product_key, geography_key)
);

CREATE INDEX idx_gold_fact_daily_date ON gold.fact_sales_daily(date_key);
CREATE INDEX idx_gold_fact_daily_product ON gold.fact_sales_daily(product_key);
CREATE INDEX idx_gold_fact_daily_geography ON gold.fact_sales_daily(geography_key);

COMMENT ON TABLE gold.fact_sales_daily IS 
'Pre-aggregated daily sales metrics for improved dashboard performance. Updated through batch process.';

{{
    config(
        materialized='view',
        schema='silver',
        tags=['staging', 'bronze', 'foundational']
    )
}}

/*
================================================================================
STAGING MODEL: stg_bronze__online_retail
================================================================================

Purpose:
    Transform raw bronze data into clean, typed, and standardized format.
    This is the foundational staging model for all downstream transformations.

Transformations Applied:
    1. Type casting from TEXT to appropriate data types
    2. Column renaming to follow naming conventions (snake_case)
    3. Basic data cleaning (trim whitespace, handle nulls)
    4. Filter out obviously invalid records
    5. Add calculated fields for data quality flags

Materialization: VIEW (ensures fresh data on each query)
Grain: One row per invoice line item (same as source)
Dependencies: source('bronze', 'online_retail_raw')

Author: Data Engineering Team
Last Updated: {{ run_started_at }}
*/

WITH source AS (
    -- Select all records from bronze source
    -- Using source() function establishes lineage
    SELECT * 
    FROM {{ source('bronze', 'online_retail_raw') }}
),

renamed_and_cast AS (
    -- ========================================================================
    -- STEP 1: Type Casting and Column Renaming
    -- ========================================================================
    -- Convert TEXT columns to appropriate data types
    -- Rename columns to follow dbt naming conventions
    -- ========================================================================
    
    SELECT
        -- Surrogate key from bronze layer
        bronze_id,
        
        -- Business keys and attributes (cast to proper types)
        TRIM(invoice) AS invoice_number,
        TRIM(stock_code) AS stock_code,
        TRIM(description) AS product_description,
        
        -- Numeric fields (handle conversion errors gracefully)
        CASE 
            WHEN quantity ~ '^-?[0-9]+$' THEN quantity::INTEGER
            ELSE NULL 
        END AS quantity,
        
        CASE 
            WHEN unit_price ~ '^[0-9]+\.?[0-9]*$' THEN unit_price::NUMERIC(10, 2)
            ELSE NULL 
        END AS unit_price,
        
        -- Date/time fields
        CASE 
            WHEN invoice_date IS NOT NULL AND invoice_date != 'nan' 
            THEN invoice_date::TIMESTAMP
            ELSE NULL 
        END AS invoice_timestamp,
        
        -- Customer and location
        CASE 
            WHEN customer_id IS NOT NULL AND customer_id != 'nan' 
            THEN customer_id::VARCHAR(50)
            ELSE NULL 
        END AS customer_id,
        
        TRIM(UPPER(country)) AS country,
        
        -- ====================================================================
        -- Metadata columns (preserved for lineage)
        -- ====================================================================
        source_file_name,
        source_file_path,
        load_timestamp,
        load_batch_id,
        record_hash
        
    FROM source
),

calculated_fields AS (
    -- ========================================================================
    -- STEP 2: Add Calculated Fields and Data Quality Flags
    -- ========================================================================
    
    SELECT
        *,
        
        -- Calculate line total (quantity × unit_price)
        CASE 
            WHEN quantity IS NOT NULL AND unit_price IS NOT NULL
            THEN quantity * unit_price
            ELSE NULL 
        END AS line_total,
        
        -- Date extraction fields for easier analysis
        DATE(invoice_timestamp) AS invoice_date,
        EXTRACT(YEAR FROM invoice_timestamp) AS invoice_year,
        EXTRACT(MONTH FROM invoice_timestamp) AS invoice_month,
        EXTRACT(DOW FROM invoice_timestamp) AS invoice_day_of_week,
        EXTRACT(HOUR FROM invoice_timestamp) AS invoice_hour,
        
        -- Flag returns (negative quantities)
        CASE 
            WHEN quantity < 0 THEN TRUE 
            ELSE FALSE 
        END AS is_return,
        
        -- Flag cancelled orders (invoice starts with 'C')
        CASE 
            WHEN invoice_number LIKE 'C%' THEN TRUE 
            ELSE FALSE 
        END AS is_cancelled,
        
        -- ====================================================================
        -- Data Quality Flags
        -- ====================================================================
        
        -- Flag records with complete data
        CASE 
            WHEN invoice_number IS NOT NULL
                AND stock_code IS NOT NULL
                AND quantity IS NOT NULL
                AND unit_price IS NOT NULL
                AND invoice_timestamp IS NOT NULL
                AND country IS NOT NULL
            THEN TRUE 
            ELSE FALSE 
        END AS is_complete_record,
        
        -- Flag records with customer information
        CASE 
            WHEN customer_id IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS has_customer_id,
        
        -- Flag records with product description
        CASE 
            WHEN product_description IS NOT NULL 
                AND LENGTH(product_description) > 0 
            THEN TRUE 
            ELSE FALSE 
        END AS has_product_description,
        
        -- Flag valid transactions (positive quantity and price)
        CASE 
            WHEN quantity > 0 AND unit_price > 0 THEN TRUE 
            ELSE FALSE 
        END AS is_valid_transaction
        
    FROM renamed_and_cast
),

final AS (
    -- ========================================================================
    -- STEP 3: Apply Final Filters and Validations
    -- ========================================================================
    -- Remove obviously invalid records
    -- Keep returns and cancellations (filtered in downstream models if needed)
    -- ========================================================================
    
    SELECT * 
    FROM calculated_fields
    WHERE 
        -- Remove records with null invoice numbers
        invoice_number IS NOT NULL
        
        -- Remove records with null or zero stock codes
        AND stock_code IS NOT NULL
        AND stock_code != ''
        
        -- Remove records with null timestamps
        AND invoice_timestamp IS NOT NULL
        
        -- Remove records with quantity = 0
        AND quantity IS NOT NULL
        AND quantity != 0
        AND quantity BETWEEN -10000 AND 10000

        -- Remove records with null or negative unit prices
        AND unit_price IS NOT NULL
        AND unit_price >= 0
)

SELECT * FROM final
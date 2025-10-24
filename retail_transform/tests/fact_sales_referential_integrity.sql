-- ============================================================================
-- CUSTOM TEST: Fact Sales Referential Integrity
-- ============================================================================
-- Purpose: Verify all fact_sales records have valid dimension keys
-- Fails if: Any orphaned records exist (missing dimension keys)
-- ============================================================================

WITH orphaned_products AS (
    -- Check for NULL product keys
    SELECT 
        'product_key' AS dimension,
        COUNT(*) AS orphan_count
    FROM {{ ref('fact_sales') }}
    WHERE product_key IS NULL
),

invalid_product_references AS (
    -- Check for product keys that don't exist in dim_product
    SELECT 
        'product_key_invalid' AS dimension,
        COUNT(*) AS orphan_count
    FROM {{ ref('fact_sales') }} f
    LEFT JOIN {{ ref('dim_product') }} p ON f.product_key = p.product_key
    WHERE f.product_key IS NOT NULL AND p.product_key IS NULL
),

orphaned_dates AS (
    -- Check for date keys that don't exist in dim_date
    SELECT 
        'date_key' AS dimension,
        COUNT(*) AS orphan_count
    FROM {{ ref('fact_sales') }} f
    LEFT JOIN {{ ref('dim_date') }} d ON f.date_key = d.date_key
    WHERE d.date_key IS NULL
),

invalid_customer_references AS (
    -- Check for customer keys that don't exist in dim_customer
    SELECT 
        'customer_key_invalid' AS dimension,
        COUNT(*) AS orphan_count
    FROM {{ ref('fact_sales') }} f
    LEFT JOIN {{ ref('dim_customer') }} c ON f.customer_key = c.customer_key
    WHERE f.customer_key IS NOT NULL AND c.customer_key IS NULL
),

all_orphans AS (
    -- Union all orphan checks
    SELECT * FROM orphaned_products
    UNION ALL
    SELECT * FROM invalid_product_references
    UNION ALL
    SELECT * FROM orphaned_dates
    UNION ALL
    SELECT * FROM invalid_customer_references
),

orphan_summary AS (
    -- Calculate total orphans across all dimensions
    SELECT 
        dimension,
        orphan_count,
        SUM(orphan_count) OVER () AS total_orphans
    FROM all_orphans
)

-- Test PASSES if no rows returned (all orphan_count = 0)
-- Test FAILS if any rows returned (orphan_count > 0)
SELECT 
    dimension,
    orphan_count,
    total_orphans
FROM orphan_summary
WHERE orphan_count > 0  -- Filter to only failing checks

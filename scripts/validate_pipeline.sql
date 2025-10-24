-- ============================================================================
-- End-to-End Pipeline Validation (CORRECTED)
-- ============================================================================

-- Verify Bronze Layer
SELECT 
    'BRONZE' AS layer,
    'online_retail_raw' AS table_name,
    COUNT(*)::TEXT AS record_count,
    COUNT(DISTINCT load_batch_id)::TEXT AS metric_2,
    MIN(load_timestamp)::TEXT AS metric_3,
    MAX(load_timestamp)::TEXT AS metric_4
FROM bronze.online_retail_raw

UNION ALL

-- Verify Silver Layer (Staging View)
SELECT 
    'SILVER' AS layer,
    'stg_bronze__online_retail' AS table_name,
    COUNT(*)::TEXT AS record_count,
    COUNT(DISTINCT invoice_number)::TEXT AS metric_2,
    MIN(invoice_date)::TEXT AS metric_3,
    MAX(invoice_date)::TEXT AS metric_4
FROM silver.stg_bronze__online_retail

UNION ALL

-- Verify Gold Layer - Dimensions
SELECT 
    'GOLD' AS layer,
    'dim_customer' AS table_name,
    COUNT(*)::TEXT AS record_count,
    AVG(total_lifetime_value)::TEXT AS metric_2,
    MIN(first_purchase_date)::TEXT AS metric_3,
    MAX(last_purchase_date)::TEXT AS metric_4
FROM gold.dim_customer

UNION ALL

SELECT 
    'GOLD' AS layer,
    'dim_product' AS table_name,
    COUNT(*)::TEXT AS record_count,
    SUM(total_revenue)::TEXT AS metric_2,
    MIN(first_sale_date)::TEXT AS metric_3,
    MAX(last_sale_date)::TEXT AS metric_4
FROM gold.dim_product

UNION ALL

SELECT 
    'GOLD' AS layer,
    'dim_date' AS table_name,
    COUNT(*)::TEXT AS record_count,
    MIN(year_number)::TEXT AS metric_2,
    MIN(full_date)::TEXT AS metric_3,
    MAX(full_date)::TEXT AS metric_4
FROM gold.dim_date

UNION ALL

-- Verify Gold Layer - Fact Table
SELECT 
    'GOLD' AS layer,
    'fact_sales' AS table_name,
    COUNT(*)::TEXT AS record_count,
    SUM(line_total)::TEXT AS metric_2,
    MIN(date_key)::TEXT AS metric_3,
    MAX(date_key)::TEXT AS metric_4
FROM gold.fact_sales;

-- ============================================================================
-- Data Quality Checks
-- ============================================================================

SELECT '========================================' AS separator;
SELECT 'DATA QUALITY CHECKS' AS section;
SELECT '========================================' AS separator;

-- Check 1: Orphaned Product Keys
SELECT 
    'Orphaned Product Keys' AS check_name,
    COUNT(*) AS failed_records,
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM gold.fact_sales f
WHERE f.product_key IS NULL;

-- Check 2: Referential Integrity - Products
SELECT 
    'Referential Integrity - Products' AS check_name,
    COUNT(*) AS failed_records,
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM gold.fact_sales f
LEFT JOIN gold.dim_product p ON f.product_key = p.product_key
WHERE f.product_key IS NOT NULL AND p.product_key IS NULL;

-- Check 3: Referential Integrity - Customers
SELECT 
    'Referential Integrity - Customers' AS check_name,
    COUNT(*) AS failed_records,
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM gold.fact_sales f
LEFT JOIN gold.dim_customer c ON f.customer_key = c.customer_key
WHERE f.customer_key IS NOT NULL AND c.customer_key IS NULL;

-- Check 4: Referential Integrity - Dates
SELECT 
    'Referential Integrity - Dates' AS check_name,
    COUNT(*) AS failed_records,
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM gold.fact_sales f
LEFT JOIN gold.dim_date d ON f.date_key = d.date_key
WHERE d.date_key IS NULL;

-- Check 5: Verify revenue calculations
SELECT 
    'Revenue Calculation Check' AS check_name,
    COUNT(*) AS failed_records,
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM gold.fact_sales
WHERE ABS(line_total - (quantity * unit_price)) > 0.01;

-- Check 6: Negative prices
SELECT 
    'Negative Unit Prices' AS check_name,
    COUNT(*) AS failed_records,
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM gold.fact_sales
WHERE unit_price < 0;

-- Check 7: NULL required fields in fact
SELECT 
    'NULL Required Fields (Fact)' AS check_name,
    COUNT(*) AS failed_records,
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM gold.fact_sales
WHERE date_key IS NULL 
   OR quantity IS NULL 
   OR unit_price IS NULL 
   OR line_total IS NULL;

-- ============================================================================
-- Detailed Investigation of Orphaned Product Keys
-- ============================================================================

SELECT '========================================' AS separator;
SELECT 'ORPHANED PRODUCT KEY ANALYSIS' AS section;
SELECT '========================================' AS separator;

-- Show sample orphaned records
SELECT 
    'Sample Orphaned Records' AS analysis,
    COUNT(*) AS total_orphans,
    COUNT(DISTINCT invoice_number) AS affected_invoices
FROM gold.fact_sales
WHERE product_key IS NULL;

-- Identify stock codes causing the issue
SELECT 
    stock_code,
    COUNT(*) AS occurrence_count
FROM silver.stg_bronze__online_retail
WHERE stock_code NOT IN (SELECT stock_code FROM gold.dim_product)
GROUP BY stock_code
ORDER BY occurrence_count DESC
LIMIT 10;

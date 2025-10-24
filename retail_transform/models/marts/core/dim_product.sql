{{
    config(
        materialized='table',
        schema='gold',
        tags=['marts', 'core', 'dimension']
    )
}}

/*
================================================================================
DIMENSIONAL MODEL: dim_product (UPDATED - Includes All Stock Codes)
================================================================================

Purpose:
    Product dimension table with performance metrics and categorization.
    NOW INCLUDES ALL stock codes to prevent orphaned fact records.

Type: SCD Type 1
Grain: One row per stock_code
*/

WITH all_products AS (
    
    -- Get ALL unique stock codes from staging (not just from intermediate)
    SELECT DISTINCT
        stock_code,
        -- Get most recent non-null description
        FIRST_VALUE(product_description) OVER (
            PARTITION BY stock_code 
            ORDER BY CASE WHEN product_description IS NOT NULL THEN 0 ELSE 1 END,
                     invoice_timestamp DESC
        ) AS product_description
    FROM {{ ref('stg_bronze__online_retail') }}
    WHERE stock_code IS NOT NULL
),

product_metrics AS (
    
    -- Left join metrics (some products may have no valid transactions)
    SELECT 
        all_products.*,
        COALESCE(metrics.unique_customers, 0) AS unique_customers,
        COALESCE(metrics.total_transactions, 0) AS total_transactions,
        COALESCE(metrics.total_units_sold, 0) AS total_units_sold,
        COALESCE(metrics.total_units_returned, 0) AS total_units_returned,
        COALESCE(metrics.total_revenue, 0) AS total_revenue,
        COALESCE(metrics.avg_unit_price, 0) AS avg_unit_price,
        COALESCE(metrics.return_count, 0) AS return_count,
        COALESCE(metrics.countries_sold_in, 0) AS countries_sold_in,
        metrics.first_sale_date,
        metrics.last_sale_date,
        metrics.days_available,
        metrics.avg_units_per_transaction,
        metrics.return_rate_percent
    FROM all_products
    LEFT JOIN {{ ref('int_product_metrics') }} metrics
        ON all_products.stock_code = metrics.stock_code
),

product_categories AS (
    
    SELECT
        *,
        
        -- Product Categorization
        CASE 
            WHEN UPPER(COALESCE(product_description, '')) LIKE '%CHRISTMAS%' 
                OR UPPER(COALESCE(product_description, '')) LIKE '%XMAS%'
            THEN 'Seasonal - Christmas'
            
            WHEN UPPER(COALESCE(product_description, '')) LIKE '%BIRTHDAY%'
            THEN 'Seasonal - Birthday'
            
            WHEN UPPER(COALESCE(product_description, '')) LIKE '%BAG%'
                OR UPPER(COALESCE(product_description, '')) LIKE '%TOTE%'
            THEN 'Bags & Accessories'
            
            WHEN UPPER(COALESCE(product_description, '')) LIKE '%HEART%'
                OR UPPER(COALESCE(product_description, '')) LIKE '%LOVE%'
            THEN 'Romantic & Gifts'
            
            WHEN UPPER(COALESCE(product_description, '')) LIKE '%VINTAGE%'
                OR UPPER(COALESCE(product_description, '')) LIKE '%RETRO%'
            THEN 'Vintage Collection'
            
            WHEN UPPER(COALESCE(product_description, '')) LIKE '%LUNCH%'
                OR UPPER(COALESCE(product_description, '')) LIKE '%KITCHEN%'
                OR UPPER(COALESCE(product_description, '')) LIKE '%PLATE%'
            THEN 'Kitchen & Dining'
            
            WHEN product_description IS NULL OR product_description = ''
            THEN 'Uncategorized - Missing Description'
            
            ELSE 'General Merchandise'
        END AS product_category,
        
        -- Performance classification
        CASE 
            WHEN total_revenue >= 10000 THEN 'Star Product'
            WHEN total_revenue >= 5000 THEN 'High Performer'
            WHEN total_revenue >= 1000 THEN 'Average Performer'
            WHEN total_revenue > 0 THEN 'Low Performer'
            ELSE 'No Sales'
        END AS performance_tier
        
    FROM product_metrics
),

final AS (
    
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['stock_code']) }} AS product_key,
        
        -- Natural key
        stock_code,
        
        -- Product attributes (with defaults for missing data)
        COALESCE(product_description, 'UNKNOWN PRODUCT') AS product_description,
        product_category,
        performance_tier,
        COALESCE(avg_unit_price, 0) AS avg_unit_price,
        
        -- Sales metrics
        unique_customers,
        total_transactions,
        total_units_sold,
        total_units_returned,
        total_revenue,
        return_count,
        COALESCE(return_rate_percent, 0) AS return_rate_percent,
        
        -- Geographic reach
        countries_sold_in,
        
        -- Temporal attributes
        first_sale_date,
        last_sale_date,
        COALESCE(days_available, 0) AS days_available,
        COALESCE(avg_units_per_transaction, 0) AS avg_units_per_transaction,
        
        -- Metadata
        CURRENT_TIMESTAMP AS updated_at,
        TRUE AS is_current
        
    FROM product_categories
)

SELECT * FROM final

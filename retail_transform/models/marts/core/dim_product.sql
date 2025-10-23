{{
    config(
        materialized='table',
        schema='gold',
        tags=['marts', 'core', 'dimension']
    )
}}

/*
================================================================================
DIMENSIONAL MODEL: dim_product
================================================================================

Purpose:
    Product dimension table with performance metrics and categorization.

Type: SCD Type 1
Grain: One row per stock_code
Dependencies: 
    - ref('int_product_metrics')
    - ref('stg_bronze__online_retail')
*/

WITH product_base AS (
    SELECT * FROM {{ ref('int_product_metrics') }}
),

product_categories AS (
    
    SELECT
        *,
        
        -- ====================================================================
        -- Product Categorization (rule-based from description patterns)
        -- ====================================================================
        
        CASE 
            WHEN UPPER(product_description) LIKE '%CHRISTMAS%' 
                OR UPPER(product_description) LIKE '%XMAS%'
            THEN 'Seasonal - Christmas'
            
            WHEN UPPER(product_description) LIKE '%BIRTHDAY%'
            THEN 'Seasonal - Birthday'
            
            WHEN UPPER(product_description) LIKE '%BAG%'
                OR UPPER(product_description) LIKE '%TOTE%'
            THEN 'Bags & Accessories'
            
            WHEN UPPER(product_description) LIKE '%HEART%'
                OR UPPER(product_description) LIKE '%LOVE%'
            THEN 'Romantic & Gifts'
            
            WHEN UPPER(product_description) LIKE '%VINTAGE%'
                OR UPPER(product_description) LIKE '%RETRO%'
            THEN 'Vintage Collection'
            
            WHEN UPPER(product_description) LIKE '%LUNCH%'
                OR UPPER(product_description) LIKE '%KITCHEN%'
                OR UPPER(product_description) LIKE '%PLATE%'
            THEN 'Kitchen & Dining'
            
            ELSE 'General Merchandise'
        END AS product_category,
        
        -- Performance classification
        CASE 
            WHEN total_revenue >= 10000 THEN 'Star Product'
            WHEN total_revenue >= 5000 THEN 'High Performer'
            WHEN total_revenue >= 1000 THEN 'Average Performer'
            ELSE 'Low Performer'
        END AS performance_tier
        
    FROM product_base
),

final AS (
    
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['stock_code']) }} AS product_key,
        
        -- Natural key
        stock_code,
        
        -- Product attributes
        product_description,
        product_category,
        performance_tier,
        avg_unit_price,
        
        -- Sales metrics
        unique_customers,
        total_transactions,
        total_units_sold,
        total_units_returned,
        total_revenue,
        return_count,
        return_rate_percent,
        
        -- Geographic reach
        countries_sold_in,
        
        -- Temporal attributes
        first_sale_date,
        last_sale_date,
        days_available,
        avg_units_per_transaction,
        
        -- Metadata
        CURRENT_TIMESTAMP AS updated_at,
        TRUE AS is_current
        
    FROM product_categories
)

SELECT * FROM final
{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'products']
    )
}}

/*
================================================================================
INTERMEDIATE MODEL: int_product_metrics
================================================================================

Purpose:
    Calculate product-level performance metrics aggregated from transactions.
    Provides product popularity, revenue contribution, and sales velocity.

Materialization: EPHEMERAL
Grain: One row per stock_code
Dependencies: ref('stg_bronze__online_retail')
*/

WITH product_transactions AS (
    
    SELECT
        stock_code,
        product_description,
        unit_price,
        quantity,
        line_total,
        invoice_date,
        country,
        customer_id,
        is_valid_transaction,
        is_return
    FROM {{ ref('stg_bronze__online_retail') }}
    WHERE is_valid_transaction = TRUE
),

product_aggregates AS (
    
    SELECT
        stock_code,
        -- Use most recent product description
        MAX(product_description) AS product_description,
        
        -- Sales metrics
        COUNT(DISTINCT CASE WHEN is_return = FALSE THEN customer_id END) AS unique_customers,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN is_return = FALSE THEN quantity ELSE 0 END) AS total_units_sold,
        SUM(CASE WHEN is_return = TRUE THEN ABS(quantity) ELSE 0 END) AS total_units_returned,
        
        -- Revenue metrics
        SUM(CASE WHEN is_return = FALSE THEN line_total ELSE 0 END) AS total_revenue,
        AVG(CASE WHEN is_return = FALSE THEN unit_price END) AS avg_unit_price,
        
        -- Return metrics
        COUNT(CASE WHEN is_return = TRUE THEN 1 END) AS return_count,
        
        -- Geographic distribution
        COUNT(DISTINCT country) AS countries_sold_in,
        
        -- Temporal metrics
        MIN(invoice_date) AS first_sale_date,
        MAX(invoice_date) AS last_sale_date
        
    FROM product_transactions
    GROUP BY stock_code
),

final AS (
    
    SELECT
        *,
        
        -- Calculate return rate
        CASE 
            WHEN total_units_sold > 0 
            THEN (total_units_returned::NUMERIC / total_units_sold) * 100
            ELSE 0
        END AS return_rate_percent,
        
        -- Calculate days product has been available
        -- EXTRACT(DAY FROM last_sale_date - first_sale_date) AS days_available,
        COALESCE(last_sale_date - first_sale_date, 0) AS days_available,

        
        -- Calculate average units per transaction
        CASE 
            WHEN total_transactions > 0 
            THEN total_units_sold::NUMERIC / total_transactions
            ELSE 0
        END AS avg_units_per_transaction
        
    FROM product_aggregates
)

SELECT * FROM final
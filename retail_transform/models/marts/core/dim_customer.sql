{{
    config(
        materialized='table',
        schema='gold',
        tags=['marts', 'core', 'dimension']
    )
}}

/*
================================================================================
DIMENSIONAL MODEL: dim_customer
================================================================================

Purpose:
    Customer dimension table for gold layer star schema.
    Contains ONE current record per customer with aggregated attributes.

Type: Slowly Changing Dimension Type 1 (overwrite current values)
Grain: One row per customer_id (UNIQUE)
Dependencies: 
    - ref('int_customer_transactions')
    - ref('stg_bronze__online_retail')
*/

WITH customer_base AS (
    
    -- Start with aggregated customer metrics from intermediate
    -- If customer has multiple countries, select the one with most transactions
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY total_orders DESC, total_lifetime_value DESC
        ) AS row_num
    FROM {{ ref('int_customer_transactions') }}
),

deduplicated_customers AS (
    -- Keep only one row per customer (highest activity)
    SELECT * 
    FROM customer_base
    WHERE row_num = 1
),

customer_segments AS (
    
    SELECT
        *,
        
        -- ====================================================================
        -- Customer Segmentation (RFM-inspired)
        -- ====================================================================
        
        -- Segment by purchase frequency
        CASE 
            WHEN total_orders >= 10 THEN 'Frequent'
            WHEN total_orders >= 5 THEN 'Regular'
            WHEN total_orders >= 2 THEN 'Occasional'
            ELSE 'One-time'
        END AS frequency_segment,
        
        -- Segment by monetary value
        CASE 
            WHEN total_lifetime_value >= 5000 THEN 'VIP'
            WHEN total_lifetime_value >= 2000 THEN 'High-Value'
            WHEN total_lifetime_value >= 500 THEN 'Medium-Value'
            ELSE 'Low-Value'
        END AS value_segment,
        
        -- Segment by recency (days since last purchase)
        CASE 
            WHEN CURRENT_DATE - last_purchase_date <= 90 THEN 'Active'
            WHEN CURRENT_DATE - last_purchase_date <= 180 THEN 'Lapsing'
            WHEN CURRENT_DATE - last_purchase_date <= 365 THEN 'Dormant'
            ELSE 'Churned'
        END AS recency_segment
        
    FROM deduplicated_customers
),

final AS (
    
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,
        
        -- Natural key
        customer_id,
        
        -- Geographic attributes
        country,
        
        -- Customer behavior metrics
        total_orders,
        total_line_items,
        total_units_purchased,
        total_lifetime_value,
        avg_order_value,
        max_order_value,
        min_order_value,
        avg_line_item_value,
        avg_days_between_orders,
        
        -- Temporal attributes
        first_purchase_date,
        last_purchase_date,
        customer_tenure_days,
        
        -- Segmentation
        frequency_segment,
        value_segment,
        recency_segment,
        
        -- Combined RFM segment
        CONCAT(
            SUBSTRING(recency_segment, 1, 1),
            '-',
            SUBSTRING(frequency_segment, 1, 1),
            '-',
            SUBSTRING(value_segment, 1, 1)
        ) AS rfm_segment,
        
        -- SCD Type 1 metadata
        CURRENT_TIMESTAMP AS updated_at,
        TRUE AS is_current
        
    FROM customer_segments
)

SELECT * FROM final
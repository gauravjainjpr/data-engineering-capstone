{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'customers']
    )
}}

/*
================================================================================
INTERMEDIATE MODEL: int_customer_transactions
================================================================================

Purpose:
    Aggregate transaction-level data to customer level, calculating
    customer lifetime metrics and behavior patterns.

Transformations:
    - Group transactions by customer_id
    - Calculate aggregate metrics (total spend, order count, avg basket)
    - Identify first and last purchase dates
    - Calculate customer tenure

Materialization: EPHEMERAL (CTE, not persisted)
Grain: One row per customer
Dependencies: ref('stg_bronze__online_retail')
*/

WITH customer_transactions AS (
    
    SELECT
        customer_id,
        invoice_number,
        invoice_date,
        invoice_timestamp,
        country,
        line_total,
        quantity,
        is_return,
        is_cancelled,
        is_valid_transaction
    FROM {{ ref('stg_bronze__online_retail') }}
    WHERE 
        customer_id IS NOT NULL  -- Only registered customers
        AND is_valid_transaction = TRUE  -- Only valid sales
        AND is_return = FALSE  -- Exclude returns
        AND is_cancelled = FALSE  -- Exclude cancelled orders
),

customer_aggregates AS (
    
    SELECT
        customer_id,
        country,
        
        -- Transaction counts
        COUNT(DISTINCT invoice_number) AS total_orders,
        COUNT(*) AS total_line_items,
        
        -- Revenue metrics
        SUM(line_total) AS total_lifetime_value,
        AVG(line_total) AS avg_line_item_value,
        SUM(quantity) AS total_units_purchased,
        
        -- Temporal metrics
        MIN(invoice_date) AS first_purchase_date,
        MAX(invoice_date) AS last_purchase_date,
        
        -- Calculate customer tenure in days
        -- EXTRACT(DAY FROM MAX(invoice_date) - MIN(invoice_date)) AS customer_tenure_days
        MAX(invoice_date) - MIN(invoice_date) AS customer_tenure_days
        
    FROM customer_transactions
    GROUP BY customer_id, country
),

customer_order_stats AS (
    
    -- Calculate per-order statistics
    SELECT
        customer_id,
        AVG(order_value) AS avg_order_value,
        MAX(order_value) AS max_order_value,
        MIN(order_value) AS min_order_value,
        STDDEV(order_value) AS stddev_order_value
    FROM (
        SELECT
            customer_id,
            invoice_number,
            SUM(line_total) AS order_value
        FROM customer_transactions
        GROUP BY customer_id, invoice_number
    ) orders
    GROUP BY customer_id
),

final AS (
    
    SELECT
        agg.*,
        stats.avg_order_value,
        stats.max_order_value,
        stats.min_order_value,
        stats.stddev_order_value,
        
        -- Calculate average days between orders
        CASE 
            WHEN agg.total_orders > 1 
            THEN agg.customer_tenure_days::NUMERIC / (agg.total_orders - 1)
            ELSE NULL
        END AS avg_days_between_orders
        
    FROM customer_aggregates agg
    LEFT JOIN customer_order_stats stats 
        ON agg.customer_id = stats.customer_id
)

SELECT * FROM final
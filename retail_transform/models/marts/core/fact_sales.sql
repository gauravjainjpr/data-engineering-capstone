{{
    config(
        materialized='incremental',
        unique_key='sale_key',
        schema='gold',
        tags=['marts', 'core', 'fact']
    )
}}

WITH transactions AS (
    SELECT
        bronze_id,
        invoice_number,
        stock_code,
        quantity,
        unit_price,
        line_total,
        invoice_date,
        invoice_timestamp,
        customer_id,
        country,
        is_return,
        is_cancelled,
        is_valid_transaction
    FROM {{ ref('stg_bronze__online_retail') }}
    
    {% if is_incremental() %}
    -- Simple date filter using staging layer
    WHERE invoice_date > (
        SELECT COALESCE(MAX(loaded_at)::date, '1900-01-01'::date)
        FROM {{ this }}
    )
    {% endif %}
)

, joined_dimensions AS (
    SELECT
        t.*,
        d.date_key,
        p.product_key,
        c.customer_key
    FROM transactions t
    INNER JOIN {{ ref('dim_date') }} d
        ON t.invoice_date = d.full_date
    LEFT JOIN {{ ref('dim_product') }} p
        ON t.stock_code = p.stock_code
    LEFT JOIN {{ ref('dim_customer') }} c
        ON t.customer_id = c.customer_id
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['bronze_id']) }} AS sale_key,
    date_key,
    product_key,
    customer_key,
    invoice_number,
    quantity,
    unit_price,
    line_total,
    0 AS discount_amount,
    line_total AS net_amount,
    is_return,
    is_cancelled,
    is_valid_transaction,
    t.bronze_id AS source_bronze_id,
    CURRENT_TIMESTAMP AS loaded_at
FROM joined_dimensions t
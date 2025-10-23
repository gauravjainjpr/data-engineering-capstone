{{
    config(
        materialized='table',
        schema='gold',
        tags=['marts', 'core', 'dimension', 'calendar']
    )
}}

/*
================================================================================
DIMENSIONAL MODEL: dim_date
================================================================================

Purpose:
    Date dimension table with comprehensive calendar attributes for
    time-based analysis and reporting.

Grain: One row per date
Range: Covers all dates in transaction history plus future dates
*/

WITH date_spine AS (
    
    -- Generate date range from earliest transaction to 1 year future
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2009-12-01' as date)",
        end_date="cast('2012-12-31' as date)"
    ) }}
),

date_attributes AS (
    
    SELECT
        date_day AS full_date,
        
        -- Date key (YYYYMMDD format for joining)
        TO_CHAR(date_day, 'YYYYMMDD')::INTEGER AS date_key,
        
        -- Calendar attributes
        EXTRACT(YEAR FROM date_day)::INTEGER AS year_number,
        EXTRACT(MONTH FROM date_day)::INTEGER AS month_number,
        TO_CHAR(date_day, 'Month') AS month_name,
        TO_CHAR(date_day, 'Mon') AS month_abbr,
        EXTRACT(DAY FROM date_day)::INTEGER AS day_of_month,
        EXTRACT(DOY FROM date_day)::INTEGER AS day_of_year,
        EXTRACT(DOW FROM date_day)::INTEGER AS day_of_week,
        TO_CHAR(date_day, 'Day') AS day_of_week_name,
        EXTRACT(WEEK FROM date_day)::INTEGER AS week_of_year,
        
        -- Quarter attributes
        EXTRACT(QUARTER FROM date_day)::INTEGER AS quarter_number,
        'Q' || EXTRACT(QUARTER FROM date_day) AS quarter_name,
        
        -- ISO week
        TO_CHAR(date_day, 'IYYY-IW') AS iso_week,
        
        -- Boolean flags
        CASE WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN date_day = CURRENT_DATE THEN TRUE ELSE FALSE END AS is_current_day,
        
        -- Fiscal period (assuming fiscal year = calendar year)
        EXTRACT(YEAR FROM date_day)::INTEGER AS fiscal_year,
        EXTRACT(QUARTER FROM date_day)::INTEGER AS fiscal_quarter,
        EXTRACT(MONTH FROM date_day)::INTEGER AS fiscal_month
        
    FROM date_spine
)

SELECT * FROM date_attributes
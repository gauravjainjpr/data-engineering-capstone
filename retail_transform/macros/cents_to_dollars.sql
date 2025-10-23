{% macro cents_to_dollars(column_name, precision=2) %}
    /*
    Convert cents to dollars with specified precision
    
    Args:
        column_name: Name of column containing cents value
        precision: Number of decimal places (default: 2)
    
    Returns:
        Numeric value in dollars
    
    Example:
        {{ cents_to_dollars('amount_cents', 2) }}
    */
    ({{ column_name }} / 100.0)::NUMERIC(18, {{ precision }})
{% endmacro %}
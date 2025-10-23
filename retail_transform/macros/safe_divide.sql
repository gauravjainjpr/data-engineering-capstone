{% macro safe_divide(numerator, denominator, default_value=0) %}
    /*
    Perform division with null/zero handling
    
    Args:
        numerator: Numerator column or expression
        denominator: Denominator column or expression
        default_value: Value to return if division by zero (default: 0)
    
    Returns:
        Result of division or default value
    */
    CASE 
        WHEN {{ denominator }} IS NULL OR {{ denominator }} = 0 
        THEN {{ default_value }}
        ELSE {{ numerator }}::NUMERIC / {{ denominator }}::NUMERIC
    END
{% endmacro %}
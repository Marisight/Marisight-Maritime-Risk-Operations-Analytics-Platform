{% macro clean_categorical(col) %}
    NULLIF(NULLIF(NULLIF(NULLIF(TRIM({{ col }}), ''), 'Unknown'), 'N/A'), '-')
{% endmacro %}

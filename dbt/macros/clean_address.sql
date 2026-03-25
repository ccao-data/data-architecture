-- Macro that aggregates CDUs to PIN-level. Strips out duplicate CDUs.
{% macro clean_address(address_columns) %}
    {% set column_name = address_columns | join(" || ' ' || ") %}
    nullif(replace(regexp_replace({{ column_name }}, '\s+', ' '), chr(0), ''), '')
{% endmacro %}

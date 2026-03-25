-- Macro that cleans and concatenates address columns by replacing multiple
-- spaces with a single space, removing null characters, and returning null
-- if the result is an empty string.
{% macro clean_address(address_columns) %}
    {% set column_name = address_columns | join(" || ' ' || ") %}
    nullif(replace(regexp_replace({{ column_name }}, '\s+', ' '), chr(0), ''), '')
{% endmacro %}

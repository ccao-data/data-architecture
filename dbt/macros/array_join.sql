-- Macro that joins array and allows user to set alias
{% macro array_join(colname, delimiter=", ", alias=None) %}
    {% set alias = alias or colname %}
    array_join({{ colname }}, '{{ delimiter }}') as {{ alias }}
{% endmacro %}

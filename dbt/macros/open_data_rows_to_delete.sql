-- Macro that aggregates CDUs to PIN-level. Strips out duplicate CDUs.
{% macro open_data_rows_to_delete(source_model, row_id_fields) %}
    select concat({{ row_id_fields | join(", ") }}) as row_id, true as ":deleted"  -- noqa
    from {{ source_model }}
    where deactivat is not null or class = '999'
{% endmacro %}

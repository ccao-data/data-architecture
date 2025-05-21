/*
Creates syntax for open data view columns year, row_id, and ":deleted". The
row_id is a unique identifier for each row in the open data views, while the
":deleted" column indicates whether a row has been deleted from
`iasworld.pardat` (or other tables that collectively define a given asset's
universe along with `pardat`) and also needs to be flagged for deletion from the
open data portal assets.

The only real complication here is that feeder views can have different columns
that define row_id, and we need to ensure that all constituents of row_id are
characters before concatenating them.
*/
{%- macro open_data_columns(row_id_cols=none) -%}
    coalesce(cast(feeder.year as int), cast(deleted_rows.year as int)) as year,
    {%- if row_id_cols is not none and "permit_number" is in row_id_cols %}
        coalesce(
            pin || coalesce(permit_number, '') || coalesce(date_issued, ''),
            cast(deleted_rows.row_id as varchar)
        ) as row_id,
    {%- elif row_id_cols is not none %}
        coalesce(
            {% for col in row_id_cols %}
                cast(feeder.{{ col }} as varchar){% if not loop.last %} ||{% else %},{% endif %}
            {% endfor %}
            cast(deleted_rows.row_id as varchar)
        ) as row_id,
    {%- else -%} coalesce(feeder.pin || feeder.year, deleted_rows.row_id) as row_id,
    {%- endif %}
    deleted_rows.":deleted"
{%- endmacro -%}

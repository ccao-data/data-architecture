/*
Macro that removes deactivated and class 999 rows from the open data views. The
only real complication here is that feeder views can have different columns that
define row_id. Currently, the only case we are accomodating is res sf/mf data,
which includes card in row_id rather than just pin and year.
*/
{%- macro open_data_columns(card=false) -%}
    coalesce(cast(feeder.year as int), cast(deleted_rows.year as int)) as year,
    {%- if card == true -%}
        coalesce(
            feeder.pin || cast(feeder.card as varchar) || feeder.year,
            deleted_rows.row_id
        ) as row_id,
    {%- else -%}
        coalesce(concat(feeder.pin, feeder.year), deleted_rows.row_id) as row_id,
    {%- endif %}
    deleted_rows.":deleted"
{%- endmacro -%}

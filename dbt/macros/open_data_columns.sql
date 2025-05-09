/*
Creates syntax for open data view columns year, row_id, and ":deleted". The
row_id is a unique identifier for each row in the open data views, while the
":deleted" column indicates whether a row has been deleted from
`iasworld.pardat` (or other tables that collectively define a given asset's
universe along with `pardat`) and also needs to be flagged for deletion from the
open data portal assets.

The only real complication here is that feeder views can have different columns
that define row_id. Currently, the only case we are accomodating is res sf/mf
data, which includes card in row_id rather than just pin and year.
*/
{%- macro open_data_columns(card=false) -%}
    coalesce(cast(feeder.year as int), cast(deleted_rows.year as int)) as year,
    {%- if card == true %}
        coalesce(
            feeder.pin || cast(feeder.card as varchar) || feeder.year,
            deleted_rows.row_id
        ) as row_id,
    {%- else -%} coalesce(feeder.pin || feeder.year, deleted_rows.row_id) as row_id,
    {%- endif %}
    deleted_rows.":deleted"
{%- endmacro -%}

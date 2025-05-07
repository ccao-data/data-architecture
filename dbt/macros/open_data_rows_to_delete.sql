/*
Macro that removes deactivated and class 999 rows from the open data views. The
only real complication here is that feeder views can have different columns that
define row_id. Currently, the only case we are accomodating is res sf/mf data,
which includes card in row_id rather than just pin and year.
*/
{%- macro open_data_rows_to_delete(card=false) -%}
    full outer join
        (
            {% if card == true -%}
                select
                    pdat.parid || cast(ddat.card as varchar) || pdat.taxyr as row_id,
                    pdat.taxyr as year,
                    true as ":deleted"
                from {{ source("iasworld", "pardat") }} as pdat
                inner join
                    {{ source("iasworld", "dweldat") }} as ddat
                    on pdat.parid = ddat.parid
                    and pdat.taxyr = ddat.taxyr
                where pdat.deactivat is not null or pdat.class = '999'
            {%- else -%}
                select parid || taxyr as row_id, taxyr as year, true as ":deleted"
                from {{ source("iasworld", "pardat") }}
                where deactivat is not null or class = '999'

            {% endif -%}
        ) as deleted_rows
        {% if card == true -%}
            on feeder.pin || cast(feeder.card as varchar) || feeder.year
            = deleted_rows.row_id
        {%- else -%} on concat(feeder.pin, feeder.year) = deleted_rows.row_id
        {%- endif -%}
{%- endmacro -%}

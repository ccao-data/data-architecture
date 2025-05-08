/*
Macro that adds deactivated and/or class 999 rows to the open data views so that
a ":deleted" flag associated with their row_id can be sent to the open data
portal.

The only real complication here is that feeder views can have different columns
that define row_id. Currently, the only case we are accomodating is res sf/mf
data, which includes card in row_id rather than just pin and year.
*/
{%- macro open_data_rows_to_delete(card=false, allow_999=false) -%}
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
                where
                    pdat.deactivat is not null
                    {%- if allow_999 == false %} or pdat.class = '999' {%- endif %}
            {%- else -%}
                select parid || taxyr as row_id, taxyr as year, true as ":deleted"
                from {{ source("iasworld", "pardat") }}
                where
                    deactivat is not null
                    {%- if allow_999 == false %} or class = '999'
                    {%- endif -%}

            {%- endif -%}
        ) as deleted_rows
        {% if card == true -%}
            on feeder.pin || cast(feeder.card as varchar) || feeder.year
            = deleted_rows.row_id
        {%- else -%} on feeder.pin || feeder.year = deleted_rows.row_id
        {%- endif -%}
{%- endmacro -%}

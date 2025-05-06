/*
Macro that removes deactivated and class 999 rows from the open data views. The
only real complication here is that feeder views can have different columns that
define row_id. Currently, the only case we are accomodating is res sf/mf data,
which includes card in row_id rather than just pin and year.
*/
{% macro open_data_rows_to_delete(feeder, card=false) %}
    deleted as (
        {%- if card == true -%}
            select
                pdat.parid || cast(ddat.card as varchar) || pdat.taxyr as row_id,
                cast(pdat.taxyr as int) as year,
                true as ":deleted"  -- noqa
            from {{ source("iasworld", "pardat") }} as pdat
            inner join
                {{ source("iasworld", "dweldat") }} as ddat
                on pdat.parid = ddat.parid
                and pdat.taxyr = ddat.taxyr
            where pdat.deactivat is not null or pdat.class = '999'
        {%- else -%}
            select
                concat(parid, taxyr) as row_id,
                cast(taxyr as int) as year,
                true as ":deleted"  -- noqa
            from {{ source("iasworld", "pardat") }}
            where deactivat is not null or class = '999'
        {%- endif -%}

    )

    select
        coalesce(
            {%- if card == true -%}
                feeder.pin
                || cast(feeder.card as varchar)
                || cast(feeder.feeder_year as varchar)
            {%- else -%}concat(feeder.pin, cast(feeder.feeder_year as varchar))
            {%- endif -%},
            deleted.row_id
        ) as row_id,
        feeder.*,
        cast(coalesce(feeder.feeder_year, deleted.year) as int) as year,
        deleted.":deleted"  -- noqa
    from feeder
    full outer join
        deleted
        on concat(feeder.pin, cast(feeder.feeder_year as varchar)) = deleted.row_id

{% endmacro %}

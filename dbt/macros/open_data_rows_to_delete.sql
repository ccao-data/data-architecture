-- Macro that aggregates CDUs to PIN-level. Strips out duplicate CDUs.
{% macro open_data_rows_to_delete(feeder, card=false) %}
    deleted as (
        {%- if card == true -%}
            select
                pdat.parid || cast(ddat.card as varchar) || pdat.taxyr as row_id,
                true as ":deleted"  -- noqa
            from {{ source("iasworld", "pardat") }} as pdat
            inner join
                {{ source("iasworld", "dweldat") }} as ddat
                on pdat.parid = ddat.parid
                and pdat.taxyr = ddat.taxyr
            where pdat.deactivat is not null or pdat.class = '999'
        {%- else -%}
            select concat(parid, taxyr) as row_id, true as ":deleted"  -- noqa
            from {{ source("iasworld", "pardat") }}
            where deactivat is not null or class = '999'
        {%- endif -%}

    )

    select
        coalesce(
            {%- if card == true -%}
                feeder.pin
                || cast(feeder.card as varchar)
                || cast(feeder.year as varchar)
            {%- else -%}concat(feeder.pin, cast(feeder.year as varchar))
            {%- endif -%},
            deleted.row_id
        ) as row_id,
        feeder.*,
        deleted.":deleted"  -- noqa
    from feeder
    full outer join
        deleted on concat(feeder.pin, cast(feeder.year as varchar)) = deleted.row_id

{% endmacro %}

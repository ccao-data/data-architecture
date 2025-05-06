-- Macro that aggregates CDUs to PIN-level. Strips out duplicate CDUs.
{% macro open_data_rows_to_delete(feeder, card=false) %}
    deleted as (
        select concat(parid, taxyr) as row_id, true as ":deleted"  -- noqa
        from {{ source("iasworld", "pardat") }}
        where deactivat is not null or class = '999'
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

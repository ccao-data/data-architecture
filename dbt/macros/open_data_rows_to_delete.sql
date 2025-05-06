-- Macro that aggregates CDUs to PIN-level. Strips out duplicate CDUs.
{% macro open_data_rows_to_delete(feeder) %}
    deleted as (
        select concat(parid, taxyr) as row_id, true as ":deleted"  -- noqa
        from {{ source("iasworld", "pardat") }}
        where deactivat is not null or class = '999'
    )

    select
        coalesce(
            concat(feeder.pin, cast(feeder.year as varchar)), deleted.row_id
        ) as row_id,
        feeder.*,
        deleted.":deleted"  -- noqa
    from feeder
    full outer join
        deleted on concat(feeder.pin, cast(feeder.year as varchar)) = deleted.row_id

{% endmacro %}

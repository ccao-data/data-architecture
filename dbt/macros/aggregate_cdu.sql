-- Macro that aggregates CDUs to PIN-level. Strips out duplicate CDUs.
{% macro aggregate_cdu(source_model, cdu_column) %}
    select
        source.parid,
        source.taxyr,
        array_join(
            array_sort(array_distinct(array_agg(upper(source.{{ cdu_column }})))), ', '
        ) as cdu_code,
        array_join(
            array_sort(array_distinct(array_agg(cdus.cdu_description))), ', '
        ) as cdu_description
    from {{ source_model }} as source
    left join
        {{ ref("ccao.cdu") }} as cdus on upper(source.{{ cdu_column }}) = cdus.cdu_code
    where
        source.{{ cdu_column }} is not null
        and source.cur = 'Y'
        and source.deactivat is null
    group by source.parid, source.taxyr
{% endmacro %}

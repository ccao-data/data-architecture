-- Macro that aggregates CDUs to PIN-level. Strips out duplicate CDUs.
{% macro aggregate_cdu(source_model, cdu_column) %}
    select
        parid,
        taxyr,
        array_join(array_sort(array_distinct(array_agg({{ cdu_column }}))), ', ') as cdu
    from {{ source_model }}
    where {{ cdu_column }} is not null and cur = 'Y' and deactivat is null
    group by parid, taxyr
{% endmacro %}

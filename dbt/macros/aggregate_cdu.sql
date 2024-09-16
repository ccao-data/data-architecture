-- Macro that aggregates CDUs to PIN-level. Strips out duplicate CDUs.
{% macro aggregate_cdu(from, cdu_column) %}
    select
        parid,
        taxyr,
        array_join(array_sort(array_distinct(array_agg({{ cdu_column }}))), ', ') as cdu
    from {{ from }}
    where {{ cdu_column }} is not null and cur = 'y' and deactivat is null
    group by parid, taxyr
{% endmacro %}

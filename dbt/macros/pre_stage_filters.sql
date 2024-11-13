-- Macros to simplify the filter conditions that produce pre-mailed and
-- pre-certified values
{% macro pre_stage_filters(tablename, stage_name) %}
    {{ tablename }}.procname is null
    and {{ tablename }}.cur = 'Y'
    and not contains(stages.procnames, '{{ stage_name }}')
{% endmacro %}

{% macro pre_mailed_filters(tablename) %}
    {{- pre_stage_filters(tablename, "CCAOVALUE") -}}
{% endmacro %}

{% macro pre_certified_filters(tablename) %}
    {{- pre_stage_filters(tablename, "CCAOFINAL") -}}
{% endmacro %}

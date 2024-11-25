-- Macros to simplify the filter conditions that produce pre-mailed and
-- pre-certified values.
--
-- Note that the `cur = 'Y'` filter is not necessary in the
-- default.vw_pin_value query that is the main consumer of these macros, since
-- that query already filters rows where `cur = 'Y'`; however, we leave the
-- filter in here so that it might make the macro more generally useful in
-- other instances where we may not filter queries the same way
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

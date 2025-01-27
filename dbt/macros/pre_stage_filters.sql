-- Macros to simplify the filter conditions that produce pre-mailed and
-- pre-certified values in the `default.vw_pin_value` view.
--
-- Note that the `cur = 'Y'` filter is not necessary in the
-- default.vw_pin_value query that is the main consumer of these macros, since
-- that query already filters rows where `cur = 'Y'`
{% macro pre_mailed_filters(tablename) %}
    {{ tablename }}.procname is null
    and {{ tablename }}.cur = 'Y'
    and cardinality(stages.procnames) = 0
{% endmacro %}

{% macro pre_certified_filters(tablename) %}
    {{ tablename }}.procname is null
    and {{ tablename }}.cur = 'Y'
    and cardinality(stages.procnames) > 0
    and not contains(stages.procnames, 'CCAOVALUE')
{% endmacro %}

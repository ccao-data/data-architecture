-- Macros to simplify the filter conditions that produce pre-mailed and
-- pre-certified values in the `default.vw_pin_value` view.
--
-- Note that the `cur = 'Y'` filter is not necessary in the
-- default.vw_pin_value query that is the main consumer of these macros, since
-- that query already filters rows where `cur = 'Y'`.
--
-- A PIN is in the pre-mailed stage for a year if none of its representations
-- in `asmt_all` in that year have procnames (i.e. stage names). We can check
-- for this status by checking the cardinality of the `stages.procnames` array,
-- where "cardinality" is the Trino SQL equivalent of the length of the array
{% macro pre_mailed_filters(tablename) %}
    {{ tablename }}.procname is null
    and {{ tablename }}.cur = 'Y'
    and cardinality(stages.procnames) = 0
{% endmacro %}

-- A PIN is the pre-certified stage for a year if it has at least one
-- representation in `asmt_all` with a procname (i.e. stage name), but none
-- of its representations in `asmt_all` have the procname that corresponds to
-- the Assessor Certified stage
{% macro pre_certified_filters(tablename) %}
    {{ tablename }}.procname is null
    and {{ tablename }}.cur = 'Y'
    and cardinality(stages.procnames) > 0
    and not contains(stages.procnames, 'CCAOFINAL')
{% endmacro %}

-- Test that a given column is a subset of a column in an external relation.
--
-- Returns rows where `column` has no matches in the external relation, with
-- additional return columns drawn from the base relation as specified by
-- `additional_select_columns`.
{% test column_is_subset_of_external_column(
    model,
    column,
    external_model,
    external_column,
    additional_select_columns=[]
) %}

    {%- set additional_select_columns_csv = additional_select_columns | join(", ") %}
    {%- set columns_csv = additional_select_columns_csv ~ ", " ~ column %}

    with
        distinct_external_values as (
            select distinct ({{ external_column }}) as external_column
            from {{ external_model }}
        )
    select {{ columns_csv }}
    from {{ model }}
    left join
        distinct_external_values dist_ext on {{ column }} = dist_ext.external_column
    where dist_ext.external_column is null

{% endtest %}

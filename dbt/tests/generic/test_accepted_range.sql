-- Override `dbt_utils.accepted_range` generic so that it can return extra
-- columns for debugging
{% test accepted_range(
    model,
    column_name,
    min_value=none,
    max_value=none,
    inclusive=true,
    additional_select_columns=[]
) %}

    {%- set columns_csv = additional_select_columns | join(", ") %}

    with
        meet_condition as (
            select {{ column_name }} {%- if columns_csv %}, {{ columns_csv }}{% endif %}
            from {{ model }}
        ),

        validation_errors as (
            select *
            from meet_condition
            where
                -- never true, defaults to an empty result set. Exists to ensure any
                -- combo of the `or` clauses below succeeds
                1 = 2

                {%- if min_value is not none %}
                    -- records with a value >= min_value are permitted. The `not`
                    -- flips this to find records that don't meet the rule.
                    or not {{ column_name }} > {{- "=" if inclusive }} {{ min_value }}
                {%- endif %}

                {%- if max_value is not none %}
                    -- records with a value <= max_value are permitted. The `not`
                    -- flips this to find records that don't meet the rule.
                    or not {{ column_name }} < {{- "=" if inclusive }} {{ max_value }}
                {%- endif %}
        )

    select *
    from validation_errors

{% endtest %}

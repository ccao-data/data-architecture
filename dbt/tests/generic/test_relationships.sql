-- Override the built-in `relationships` generic so that it can return extra
-- columns for debugging
{% test relationships(model, column_name, to, field, select_columns=[]) %}

    {%- set columns_csv = select_columns | join(", ") %}

    with
        child as (
            select
                {{ column_name }} as from_field
                {%- if columns_csv %}, {{ columns_csv }}{% endif %}
            from {{ model }}
            where {{ column_name }} is not null
        ),

        parent as (select {{ field }} as to_field from {{ to }})

    select
        from_field as {{ column_name }}
        {%- if columns_csv %}, {{ columns_csv }}{% endif %}
    from child
    left join parent on child.from_field = parent.to_field

    where parent.to_field is null

{% endtest %}

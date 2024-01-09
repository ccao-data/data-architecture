-- Override the built-in `relationships` generic so that it can return extra
-- columns for debugging
{% test relationships(model, column_name, to, field, additional_select_columns=[]) %}

    {%- set columns_csv = additional_select_columns | join(", ") %}

    with
        child as (
            select {{ column_name }} as from_field, *
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

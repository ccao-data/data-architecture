-- For all residential parcels in a given model, test that there is at least one
-- class code that matches a class code for that parcel in pardat.
--
-- By default, the test will compare the first 3 digits of each set of classes;
-- if `major_class_only=true`, however, the test will compare the first digit
-- only.
--
-- The test filters for residential parcels by anti-joining the model against
-- `comdat`. This join will be performed using (parid, taxyr, card) by default,
-- but the join can be configured to use (parid, taxyr) instead by setting
-- `filter_for_res_by_card=false`.
{% test res_class_matches_pardat(
    model,
    column_name,
    filter_for_res_by_card=true,
    major_class_only=false,
    additional_select_columns=[]
) %}

    {%- set num_class_digits = 1 if major_class_only else 3 %}

    with
        filtered_model as (
            select child.*
            from (select * from {{ model }}) as child
            left join
                {{ source("iasworld", "comdat") }} as comdat
                on child.parid = comdat.parid
                and child.taxyr = comdat.taxyr
                {%- if filter_for_res_by_card %} and child.card = comdat.card{% endif %}
            where comdat.parid is null
        )

    select
        array_agg(distinct(filtered_model.{{ column_name }})) as {{ column_name }},
        {%- for col in additional_select_columns %}
            max(filtered_model.{{ col }}) as {{ col }},
        {%- endfor %}
        max(pardat.class) as pardat_class
    from filtered_model
    left join
        (
            select *
            from {{ source("iasworld", "pardat") }}
            where cur = 'Y' and deactivat is null
        ) as pardat
        on pardat.parid = filtered_model.parid
        and pardat.taxyr = filtered_model.taxyr
    group by filtered_model.parid, filtered_model.taxyr
    having
        sum(
            case
                when
                    substr(pardat.class, 1, {{ num_class_digits }}) = substr(
                        filtered_model.{{ column_name }}, 1, {{ num_class_digits }}
                    )
                then 1
                else 0
            end
        )
        = 0
{% endtest %}

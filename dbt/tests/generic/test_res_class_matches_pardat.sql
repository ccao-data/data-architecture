-- Test that residential class codes match pardat. The test will compare full
-- classes unless `major_class_only=true`. "Residential" parcels are
-- determined by anti-joining against `comdat`; by default this join will be
-- performed using (parid, taxyr, card), but the join can be configured to
-- use (parid, taxyr) by setting `filter_for_res_by_card=false`
{% test res_class_matches_pardat(
    model,
    column_name,
    filter_for_res_by_card=true,
    major_class_only=false,
    additional_select_columns=[]
) %}

    {%- set columns_csv = additional_select_columns | join(", ") %}
    {%- set num_class_digits = 1 if major_class_only else 3 %}

    with
        res_child as (
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
        parid,
        taxyr,
        {%- if columns_csv %} {{ columns_csv }},{% endif %}
        array_agg(res_child.{{ column_name }}) as {{ column_name }},
        -- pardat is unique by (taxyr, parid) so all classes should be the same
        max(pardat.class) as pardat_class
    from res_child
    left join {{ source("iasworld", "pardat") }} as pardat using (parid, taxyr)
    group by parid, taxyr
    having
        sum(
            case
                when
                    substr(pardat.class, {{ num_class_digits }})
                    = substr(res_child.{{ column_name }}, {{ num_class_digits }})
                then 1
                else 0
            end
        )
        = 0
{% endtest %}

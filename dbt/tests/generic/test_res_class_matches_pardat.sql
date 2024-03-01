-- For all residential parcels in a given model, test that there is at least one
-- class code that matches a class code for that parcel in pardat. The test
-- filters for residential parcels by anti-joining the model against `comdat`
-- using parid and taxyr; as a result, it filters out mixed-use parcels as well.
--
-- By default, the test will compare the first 3 digits of each set of classes;
-- if `major_class_only=true`, however, the test will compare the first digit
-- only.
--
-- Since the output is grouped by (parid, taxyr), additional columns that would
-- normally be selected via `additional_select_columns` need to be selected
-- with one of two aggregation methods: `select_columns_aggregated_with_array`
-- (which uses the `array_agg()` function to select the columns) or
-- `select_columns_aggregated_with_max` (which uses the `max()` function).
{% test res_class_matches_pardat(
    model,
    column_name,
    major_class_only=false,
    select_columns_aggregated_with_array=[],
    select_columns_aggregated_with_max=[]
) %}

    {%- set num_class_digits = 1 if major_class_only else 3 %}

    with
        filtered_model as (
            select model.*
            from (select * from {{ model }}) as model
            left join
                (
                    select distinct parid, taxyr
                    from {{ source("iasworld", "comdat") }}
                    where comdat.cur = 'Y' and comdat.deactivat is null
                ) as comdat
                on model.parid = comdat.parid
                and model.taxyr = comdat.taxyr
            where comdat.parid is null
        )

    select
        array_agg(filtered_model.{{ column_name }}) as {{ column_name }},
        {%- for col in select_columns_aggregated_with_array %}
            array_agg(filtered_model.{{ col }}) as {{ col }},
        {%- endfor %}
        {%- for col in select_columns_aggregated_with_max %}
            max(filtered_model.{{ col }}) as {{ col }},
        {%- endfor %}
        -- Pardat should be unique by (parid, taxyr), so we can select the
        -- max rather than array_agg
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

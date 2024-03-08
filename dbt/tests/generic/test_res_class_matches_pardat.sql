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
    model, column_name, major_class_only=false, additional_select_columns=[]
) %}
    -- Process `additional_select_columns` to add the necessary prefix for
    -- the filtered version of the input model. This is necessary to
    -- disambiguate selected columns that are shared between the input model
    -- and pardat
    {%- set processed_additional_select_columns = [] %}
    {%- for col in additional_select_columns %}
        {%- if col is mapping %}
            {%- set updated_col = {"column": "filtered_model." ~ col.column} %}
            {%- for key, val in col.items() if key != "column" %}
                {%- set _ = updated_col.update({key: val}) %}
            {%- endfor %}
            -- It's necessary to explicitly set the alias, since otherwise
            -- it will default to `filtered_model.{column}` according to
            -- the fallback behavior of format_additional_select_columns
            {%- if "alias" not in updated_col %}
                {%- set _ = updated_col.update({"alias": col.column}) %}
            {%- endif %}
            {%- set _ = processed_additional_select_columns.append(updated_col) %}
        {%- else %}
            {%- set _ = processed_additional_select_columns.append(
                "filtered_model." ~ col
            ) %}
        {%- endif %}
    {%- endfor %}
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
        {{ format_additional_select_columns(processed_additional_select_columns) }},
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

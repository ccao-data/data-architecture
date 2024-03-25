-- For all residential parcels in a given model, test that there is at least one
-- class code that matches a class code for that parcel in pardat. The test
-- filters for residential parcels by anti-joining the model against `comdat`
-- using parid and taxyr; as a result, it filters out mixed-use parcels as well.
--
-- Optional parameters:
--
-- * major_class_only (bool): Compare only the first digit of classes. When
-- set to False, compare the first three digits instead.
-- * parid_column_name (str): The name of the column on the base model that
-- corresponds to `parid`, in case the model uses a different name scheme.
-- * taxyr_column_name (str): The name of the column on the base model that
-- corresponds to `taxyr`, in case the model uses a different name scheme.
-- * additional_select_columns (dict): Standard parameter for selecting
-- additional columns from the base model for use in reporting failures.
-- `model.{column_name}` and `pardat.class` are already selected by
-- default and do not need to be included using this parameter.
-- See the `format_additional_select_columns` macro for more information.
-- Note that while this parameter can often be either a string or a dict,
-- a dict is required for this test and it must contain the `alias`
-- and `agg_func` parameters given that results are grouped by `parid`
-- and `taxyr`.
--
{% test res_class_matches_pardat(
    model,
    column_name,
    major_class_only=false,
    parid_column_name="parid",
    taxyr_column_name="taxyr",
    additional_select_columns=[]
) %}
    {#-
        Process `additional_select_columns` to add the necessary prefix for
        the filtered version of the input model. This is necessary to
        disambiguate selected columns that are shared between the input model
        and pardat
    -#}
    {%- set processed_additional_select_columns = [] %}
    {%- for col in additional_select_columns %}
        {%- if col is mapping %}
            {%- set updated_col = {"column": "filtered_model." ~ col.column} %}
            {%- for key, val in col.items() if key != "column" %}
                {%- set _ = updated_col.update({key: val}) %}
            {%- endfor %}
            {#-
                It's necessary to explicitly set the alias, since otherwise
                it will default to `filtered_model.{column}` according to
                the fallback behavior of format_additional_select_columns
            -#}
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
                on model.{{ parid_column_name }} = comdat.parid
                and model.{{ taxyr_column_name }} = comdat.taxyr
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
        on filtered_model.{{ parid_column_name }} = pardat.parid
        and filtered_model.{{ taxyr_column_name }} = pardat.taxyr
    group by
        filtered_model.{{ parid_column_name }}, filtered_model.{{ taxyr_column_name }}
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

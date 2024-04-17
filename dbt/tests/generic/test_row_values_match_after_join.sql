-- fmt: off
--
-- Test that row values match after joining two tables. Row values can be
-- a subset of the values in the joined table, i.e. if the class of PINA
-- from tableA is '212' and the class of PINA from tableB is '211' and '212',
-- then tableA matches (returns no rows).
--
-- Required parameters:
--
--    * external_model (str): The name of the model to join to.
--
--    * external_column_name (str): The name of the column in `external_model`
--    to join to.
--
--    * join_condition (str): The ON (or USING) portion of a JOIN clause,
--    represented as a string. Note that in the case where ON is used,
--    columns in the base model should be formatted like `model.{column}`
--    while columns in the external model should be formatted like
--    `external_model.{column}`, e.g. `ON model.pin = external_model.parid`.
--    This is not necessary in the case of USING, since USING does not
--    refer to tablenames directly.
--
--    * group_by (list of str): The columns from the base model to pass to the
--    GROUP BY function used in the test query. Unlike `join_condition`,
--    these column names do not have to be prefixed with `model.*`, since
--    they are assumed to come from the base model for the test and not the
--    external model.
--
-- Optional parameters:
--
--    * join_type(str): The type of join to use, e.g. "inner" or "left".
--    Defaults to "inner".
--
--    * column_alias (str): An alias to use when selecting the column from the
--    base model for output. An alias is required in this case because
--    the column must be aggregated. Defaults to "model_col".
--
--    * external_column_alias (str): An alias to use when selecting the column
--    from the external model for output. Defaults to "external_model_col".
--
--    * additional_select_columns (dict): Standard parameter for selecting
--    additional columns from the base model for use in reporting failures.
--    `model.{column_name}`, `external_model.{external_column_name`}`, and
--    the columns specified in the `group_by` parameter are already selected
--    by default and do not need to be included using this parameter.
--    See the `format_additional_select_columns` macro for more information.
--    Note that while this parameter can often be either a string or a dict,
--    a dict is required for this test and it must contain the `alias`
--    and `agg_func` parameters given that results are grouped by the
--    columns specified in `group_by`.
--
-- fmt: on
{% test row_values_match_after_join(
    model,
    column_name,
    external_model,
    external_column_name,
    join_condition,
    group_by,
    join_type="inner",
    column_alias="model_col",
    external_column_alias="external_model_col",
    additional_select_columns=[]
) %}

    {%- set additional_select_columns_csv = format_additional_select_columns(
        additional_select_columns
    ) -%}

    {%- set group_by_qualified = [] -%}
    {%- for col in group_by -%}
        {%- set _ = group_by_qualified.append("model." ~ col) -%}
    {%- endfor -%}
    {%- set group_by_csv = group_by_qualified | join(", ") -%}

    {%- if "." in column_name -%} {%- set model_col = column_name -%}
    {%- else -%} {%- set model_col = "model." ~ column_name -%}
    {%- endif -%}

    {%- if "." in external_column_name -%}
        {%- set external_model_col = external_column_name -%}

    {%- else -%}

        {%- set external_model_col = "external_model." ~ external_column_name -%}

    {%- endif -%}

    select
        {{ group_by_csv }},
        {% if additional_select_columns_csv -%}
            {{ additional_select_columns_csv }},
        {% endif %}
        array_agg({{ model_col }}) as {{ column_alias }},
        array_agg({{ external_model_col }}) as {{ external_column_alias }}
    from {{ external_model }} as external_model {{ join_type }}
    join (select * from {{ model }}) as model {{ join_condition }}
    group by {{ group_by_csv }}
    having
        sum(case when {{ external_model_col }} = {{ model_col }} then 1 else 0 end) = 0

{% endtest %}

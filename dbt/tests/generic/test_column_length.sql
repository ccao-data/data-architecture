-- Test that all columns in a list have the correct length.
--
-- Returns columns with the prefix `len_` representing the length of each
-- columns for all rows where one of the columns has an incorrect length.
--
-- If additional columns should be returned along with the length columns,
-- e.g. to add an identifiable key for the row, use the
-- additional_select_columns argument.
{% test column_length(model, columns, length, additional_select_columns=[]) %}

    {%- set additional_select_columns_csv = format_additional_select_columns(
        additional_select_columns
    ) %}

    {%- set length_columns = [] %}
    {%- for column in columns %}
        {%- set length_columns = length_columns.append([column, "len_" + column]) %}
    {%- endfor %}

    {%- set select_columns = [] %}
    {%- set filter_conditions = [] %}

    {%- for column, length_column in length_columns %}
        {%- set select_columns = select_columns.append(
            "length(" + column + ") as " + length_column
        ) %}
        {%- set filter_conditions = filter_conditions.append(
            "("
            + length_column
            + " is not null and "
            + length_column
            + " > "
            + length
            | string + ")"
        ) %}
    {%- endfor %}

    {%- set columns_csv = select_columns | join(", ") %}
    {%- set filter_conditions_str = filter_conditions | join(" or ") %}

    with
        column_lengths as (
            select
                {{ columns_csv }}
                {%- if additional_select_columns_csv -%}
                    , {{ additional_select_columns_csv }}
                {%- endif %}
            from {{ model }}
        )
    select *
    from column_lengths
    where {{ filter_conditions_str }}

{% endtest %}

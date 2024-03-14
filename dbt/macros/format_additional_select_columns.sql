-- Format a list of strings or objects representing columns that should be
-- selected in a test, such that the columns can be templated into a SELECT
-- statement.
--
-- If an element of the list is a string, the column it represents will
-- be selected as-is. If instead the element is a dictionary, it can contain
-- the following key-value pairs:
--
-- * `column` (required string): The name of the column to select
-- * `alias` (optional string): The alias to use for the column
-- (defaults to `column`)
-- * `agg_func` (optional string): An aggregation function to use to
-- select the column (defaults to no aggregation)
{% macro format_additional_select_columns(additional_select_columns) %}
    -- Pass execution off to a helper function with a configurable error
    -- handler, to make it possible to unit test exceptions
    {{
        return(
            _format_additional_select_columns(
                additional_select_columns, exceptions.raise_compiler_error
            )
        )
    }}
{% endmacro %}

{% macro _format_additional_select_columns(
    additional_select_columns, raise_error_func
) %}
    {%- for col in additional_select_columns -%}
        {%- if col is mapping -%}
            {%- if "column" not in col -%}
                {{-
                    return(
                        raise_error_func(
                            'Missing required "column" key in config: ' ~ col
                        )
                    )
                -}}
            {%- else -%}
                {%- if col.agg_func -%}
                    {%- set alias = col.alias if col.alias else col.column -%}
                    {{- col.agg_func }} ({{ col.column }}) as {{ alias }}
                {%- else -%}
                    {%- if col.alias -%} {{- col.column }} as {{ col.alias }}
                    {%- else -%} {{- col.column }}
                    {%- endif -%}
                {%- endif -%}
            {%- endif -%}
        {%- else -%} {{ col }}
        {%- endif -%}
        {{- "" if loop.last else "," }}
    {%- endfor -%}
{% endmacro %}

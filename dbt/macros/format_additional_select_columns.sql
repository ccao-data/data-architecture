-- Format a list of strings or objects representing columns that should be
-- selected in a test, such that the columns can be templated into a SELECT
-- statement.
--
-- If an element of the list is a string, the column it represents will
-- be selected using the name of the column as an alias. If instead the
-- element is a dictionary, it can contain the following key-value
-- pairs:
-- * `column` (required string): The name of the column to select
-- * `alias` (optional string): The alias to use for the column
-- (defaults to `column`)
-- * `agg_func` (optional string): An aggregation function to use to
-- select the column (defaults to no aggregation)
{% macro format_additional_select_columns(additional_select_columns) %}
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
        {%- set trailing_comma = "" if loop.last else "," %}
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
                {%- set label = col.label if col.label else col.column -%}
                {%- if col.agg_func -%}
                    {{- col.agg_func }} ({{ col.column }}) as {{ label }}
                    {{- trailing_comma }}
                {%- else -%} {{- col.column }} as {{ label }}{{ trailing_comma }}
                {%- endif -%}
            {%- endif -%}
        {%- else -%} {{ col }} as {{ col }}{{ trailing_comma }}
        {%- endif -%}
    {%- endfor -%}
{% endmacro %}

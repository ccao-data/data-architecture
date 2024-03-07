{% macro test_format_additional_select_columns() %}
    {% do test_format_additional_select_columns_string_element() %}
    {% do test_format_additional_select_columns_string_list() %}
    {% do test_format_additional_select_columns_dict_element_no_column() %}
    {% do test_format_additional_select_columns_dict_element_no_label() %}
    {% do test_format_additional_select_columns_dict_element_with_label() %}
    {% do test_format_additional_select_columns_dict_element_array_agg() %}
{% endmacro %}

{% macro test_format_additional_select_columns_string_element() %}
    {{
        assert_equals(
            "test_format_additional_select_columns_string_element",
            format_additional_select_columns(["foo"]),
            "foo as foo,",
        )
    }}
{% endmacro %}

{% macro test_format_additional_select_columns_string_list() %}
    {{
        assert_equals(
            "test_format_additional_select_columns_string_element",
            format_additional_select_columns(["foo", "bar"]),
            "foo as foo,bar as bar,",
        )
    }}
{% endmacro %}

{% macro test_format_additional_select_columns_dict_element_no_column() %}
    {{
        assert_equals(
            "test_format_additional_select_columns_dict_element_no_column",
            _format_additional_select_columns(
                [{"label": "foo"}], mock_raise_compiler_error
            ),
            "Missing required \"column\" key in config: {'label': 'foo'}",
        )
    }}
{% endmacro %}

{% macro test_format_additional_select_columns_dict_element_no_label() %}
    {{
        assert_equals(
            "test_format_additional_select_columns_dict_element_no_label",
            format_additional_select_columns([{"column": "foo"}]),
            "foo as foo,",
        )
    }}
{% endmacro %}

{% macro test_format_additional_select_columns_dict_element_with_label() %}
    {{
        assert_equals(
            "test_format_additional_select_columns_dict_element_with_label",
            format_additional_select_columns([{"column": "foo", "label": "bar"}]),
            "foo as bar,",
        )
    }}
{% endmacro %}

{% macro test_format_additional_select_columns_dict_element_array_agg() %}
    {{
        assert_equals(
            "test_format_additional_select_columns_dict_element_array_agg",
            format_additional_select_columns([{"column": "foo", "agg_func": "max"}]),
            "max (foo) as foo,",
        )
    }}
{% endmacro %}

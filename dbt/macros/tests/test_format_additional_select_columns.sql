{% macro test_format_additional_select_columns() %}
    {% do test_format_additional_select_columns_string_element() %}
    {% do test_format_additional_select_columns_string_element_with_alias() %}
    {% do test_format_additional_select_columns_string_list() %}
    {% do test_format_additional_select_columns_dict_element_no_column() %}
    {% do test_format_additional_select_columns_dict_element_no_alias() %}
    {% do test_format_additional_select_columns_dict_element_with_alias() %}
    {% do test_format_additional_select_columns_dict_element_array_agg() %}
    {% do test_format_additional_select_columns_dict_element_array_w_alias() %}
{% endmacro %}

{% macro test_format_additional_select_columns_string_element() %}
    {{
        assert_equals(
            "test_format_additional_select_columns_string_element",
            format_additional_select_columns(["foo"]),
            "foo",
        )
    }}
{% endmacro %}

{% macro test_format_additional_select_columns_string_element_with_alias() %}
    {{
        assert_equals(
            "test_format_additional_select_columns_string_element_with_alias",
            format_additional_select_columns(["foo as bar"]),
            "foo as bar",
        )
    }}
{% endmacro %}

{% macro test_format_additional_select_columns_string_list() %}
    {{
        assert_equals(
            "test_format_additional_select_columns_string_element",
            format_additional_select_columns(["foo", "bar"]),
            "foo,bar",
        )
    }}
{% endmacro %}

{% macro test_format_additional_select_columns_dict_element_no_column() %}
    {{
        assert_equals(
            "test_format_additional_select_columns_dict_element_no_column",
            _format_additional_select_columns(
                [{"alias": "foo"}], mock_raise_compiler_error
            ),
            "Missing required \"column\" key in config: {'alias': 'foo'}",
        )
    }}
{% endmacro %}

{% macro test_format_additional_select_columns_dict_element_no_alias() %}
    {{
        assert_equals(
            "test_format_additional_select_columns_dict_element_no_alias",
            format_additional_select_columns([{"column": "foo"}]),
            "foo",
        )
    }}
{% endmacro %}

{% macro test_format_additional_select_columns_dict_element_with_alias() %}
    {{
        assert_equals(
            "test_format_additional_select_columns_dict_element_with_alias",
            format_additional_select_columns([{"column": "foo", "alias": "bar"}]),
            "foo as bar",
        )
    }}
{% endmacro %}

{% macro test_format_additional_select_columns_dict_element_array_agg() %}
    {{
        assert_equals(
            "test_format_additional_select_columns_dict_element_array_agg",
            format_additional_select_columns([{"column": "foo", "agg_func": "max"}]),
            "max (foo) as foo",
        )
    }}
{% endmacro %}

{% macro test_format_additional_select_columns_dict_element_array_w_alias() %}
    {{
        assert_equals(
            "test_format_additional_select_columns_dict_element_array_w_alias",
            format_additional_select_columns(
                [{"column": "foo", "alias": "bar", "agg_func": "max"}]
            ),
            "max (foo) as bar",
        )
    }}
{% endmacro %}

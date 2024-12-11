{% macro test_insert_hyphens() %}
    {% do test_insert_hyphens_one_position() %}
    {% do test_insert_hyphens_multiple_positions() %}
    {% do test_insert_hyphens_raises_when_missing_arguments() %}
{% endmacro %}

{% macro test_insert_hyphens_one_position() %}
    {{
        assert_equals(
            "test_insert_hyphens_one_position",
            insert_hyphens("class", 1),
            "concat(substr(class, 1, 1),'-',substr(class, 2))",
        )
    }}
{% endmacro %}

{% macro test_insert_hyphens_multiple_positions() %}
    {{
        assert_equals(
            "test_insert_hyphens_multiple_positions",
            insert_hyphens("pin", 2, 4, 7, 10),
            "concat(substr(pin, 1, 2),'-',substr(pin, 3, 2),'-',substr(pin, 5, 3),'-',substr(pin, 8, 3),'-',substr(pin, 11))",
        )
    }}
{% endmacro %}

{% macro test_insert_hyphens_raises_when_missing_arguments() %}
    {{
        assert_equals(
            "test_insert_hyphens_raises_when_missing_arguments",
            _insert_hyphens("pin", [], mock_raise_compiler_error),
            "insert_hyphens expects one or more positional arguments",
        )
    }}
{% endmacro %}

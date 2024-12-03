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
            "CONCAT(SUBSTR(class, 1, 1),'-',SUBSTR(class, 2))"
        )
    }}
{% endmacro %}

{% macro test_insert_hyphens_multiple_positions() %}
    {{
        assert_equals(
            "test_insert_hyphens_multiple_positions",
            insert_hyphens("pin", 2, 4, 7, 10),
            "CONCAT(SUBSTR(pin, 1, 2),'-',SUBSTR(pin, 3, 2),'-',SUBSTR(pin, 5, 3),'-',SUBSTR(pin, 8, 3),'-',SUBSTR(pin, 11))"
        )
    }}
{% endmacro %}

{% macro test_insert_hyphens_raises_when_missing_arguments() %}
    {{
        assert_equals(
            "test_insert_hyphens_raises_when_missing_arguments",
            _insert_hyphens("pin", [], mock_raise_compiler_error),
            "insert_hyphens expects one or more positional arguments"
        )
    }}
{% endmacro %}

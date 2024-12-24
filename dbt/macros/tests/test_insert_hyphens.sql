{% macro test_insert_hyphens() %}
    {% do test_insert_hyphens_one_position() %}
    {% do test_insert_hyphens_multiple_positions() %}
    {% do test_insert_hyphens_raises_when_missing_arguments() %}
{% endmacro %}

{% macro test_insert_hyphens_one_position() %}
    {{
        assert_equals(
            "test_insert_hyphens_one_position",
            insert_hyphens("pardat.class", 1),
            "concat(substr(pardat.class, 1, 1),'-',substr(pardat.class, 2))",
        )
    }}
{% endmacro %}

{% macro test_insert_hyphens_multiple_positions() %}
    {{
        assert_equals(
            "test_insert_hyphens_multiple_positions",
            insert_hyphens("pardat.pin", 2, 4, 7, 10),
            "concat(substr(pardat.pin, 1, 2),'-',substr(pardat.pin, 3, 2),'-',substr(pardat.pin, 5, 3),'-',substr(pardat.pin, 8, 3),'-',substr(pardat.pin, 11))",
        )
    }}
{% endmacro %}

{% macro test_insert_hyphens_str_as_string() %}
    {{
        assert_equals(
            "test_insert_hyphens_multiple_positions",
            insert_hyphens("'foobar'", 3),
            "concat(substr('foobar', 1, 3),'-',substr('foobar', 4))",
        )
    }}
{% endmacro %}

{% macro test_insert_hyphens_raises_when_missing_arguments() %}
    {{
        assert_equals(
            "test_insert_hyphens_raises_when_missing_arguments",
            _insert_hyphens("pardat.pin", [], mock_raise_compiler_error),
            "insert_hyphens expects one or more positional arguments",
        )
    }}
{% endmacro %}

{% macro test_insert_hyphens_raises_when_str_is_not_a_string() %}
    {{
        assert_equals(
            "test_insert_hyphens_raises_when_str_is_not_a_string",
            _insert_hyphens(1, [2, "pardat.pin"], [], mock_raise_compiler_error),
            "insert_hyphens expects the first argument to be a string to insert hyphens into",
        )
    }}
{% endmacro %}

{% macro test_insert_hyphens_raises_when_varargs_are_not_ints() %}
    {{
        assert_equals(
            "test_insert_hyphens_raises_when_varargs_are_not_ints",
            _insert_hyphens("pardat.pin", [1, "foo"], [], mock_raise_compiler_error),
            "insert_hyphens expects all positional arguments to be integers",
        )
    }}
{% endmacro %}

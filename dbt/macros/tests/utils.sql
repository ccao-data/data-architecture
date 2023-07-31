{% macro assert_equals(test_name, value, expected) %}
    {% if value == expected %}
        {% do log(test_name ~ " - PASS", info=True) %}
    {% else %}
        {% do exceptions.raise_compiler_error(
            test_name ~ " - FAIL: " ~ value~ " != " ~ expected
        ) %}
    {% endif %}
{% endmacro %}

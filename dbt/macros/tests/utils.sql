-- Helper function to assert that a value is equal to an expected value.
-- Requires a `test_name` argument so that the test name can be logged
-- to the console with its status
{% macro assert_equals(test_name, value, expected) %}
    {% if value == expected %} {% do log(test_name ~ " - PASS", info=True) %}
    {% else %}
        {% do exceptions.raise_compiler_error(
            test_name ~ " - FAIL: " ~ value ~ " != " ~ expected
        ) %}
    {% endif %}
{% endmacro %}

-- Mock that can take the place of exceptions.raise_compiler_error and
-- return the error for equality comparison instead of raising it
{% macro mock_raise_compiler_error(_error) %} {{ return(_error) }} {% endmacro %}

-- Mock that spoofs the existence of USER and HEAD_REF env vars
{% macro mock_env_var(var_name) %}
    {% if var_name == "USER" %} {{ return("test-user") }}
    {% elif var_name == "HEAD_REF" %} {{ return("testuser/feature-branch-1") }}
    {% else %} {{ return("") }}
    {% endif %}
{% endmacro %}

-- Mock that spoofs a situation in which all env vars are unset
{% macro mock_no_env_var(var_name) %} {{ return("") }} {% endmacro %}

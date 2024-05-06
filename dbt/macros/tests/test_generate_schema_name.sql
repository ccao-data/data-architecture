{% macro test_generate_schema_name() %}
    {% do test_generate_schema_name_handles_dev_env() %}
    {% do test_generate_schema_name_handles_ci_env() %}
    {% do test_generate_schema_name_handles_prod_env() %}
    {% do test_generate_schema_name_raises_for_default_schema_name() %}
{% endmacro %}

{% macro mock_env_var(var_name) %}
    {% if var_name == "USER" %} {{ return("test-user") }}
    {% elif var_name == "HEAD_REF" %} {{ return("testuser/feature-branch-1") }}
    {% else %} {{ return("") }}
    {% endif %}
{% endmacro %}

{% macro test_generate_schema_name_handles_dev_env() %}
    {% do assert_equals(
        "test_generate_schema_name_handles_dev_env",
        _generate_schema_name(
            "test",
            {"name": "test"},
            {"schema": "default", "name": "dev"},
            mock_env_var,
            exceptions.raise_compiler_error,
        ),
        "z_dev_test-user_test",
    ) %}
{% endmacro %}

{% macro test_generate_schema_name_handles_ci_env() %}
    {% do assert_equals(
        "test_generate_schema_name_handles_ci_env",
        _generate_schema_name(
            "test",
            {"name": "test"},
            {"schema": "default", "name": "ci"},
            mock_env_var,
            exceptions.raise_compiler_error,
        ),
        "z_ci_testuser_feature_branch_1_test",
    ) %}
{% endmacro %}

{% macro test_generate_schema_name_handles_prod_env() %}
    {% do assert_equals(
        "test_generate_schema_name_handles_prod_env",
        _generate_schema_name(
            "test",
            {"name": "test"},
            {"schema": "default", "name": "prod"},
            mock_env_var,
            exceptions.raise_compiler_error,
        ),
        "test",
    ) %}
{% endmacro %}

{% macro test_generate_schema_name_raises_for_default_schema_name() %}
    {% do assert_equals(
        "test_generate_schema_name_raises_for_default_schema_name",
        _generate_schema_name(
            None,
            {"name": "test"},
            {"schema": "default", "name": "prod"},
            mock_env_var,
            mock_raise_compiler_error,
        ),
        (
            "Missing schema definition for test. Its containing subdirectory "
            "is probably missing a `+schema` attribute under the `models` "
            "config in dbt_project.yml."
        ),
    ) %}
{% endmacro %}

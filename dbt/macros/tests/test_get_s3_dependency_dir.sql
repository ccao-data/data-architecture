{% macro test_get_s3_dependency_dir() %}
    {% do test_get_s3_dependency_dir_target_dev() %}
    {% do test_get_s3_dependency_dir_target_not_dev() %}
    {% do test_get_s3_dependency_dir_target_dev_raises_wo_username() %}
    {% do test_get_s3_dependency_dir_target_not_dev_wont_raise_wo_username() %}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_dev() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_dev",
            _get_s3_dependency_dir(
                "dev", "s3://bucket/", "username", exceptions.raise_compiler_error
            ),
            "s3://bucket/packages/username",
        )
    }}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_not_dev() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_not_dev",
            _get_s3_dependency_dir(
                "ci", "s3://bucket/", "username", exceptions.raise_compiler_error
            ),
            "s3://bucket/packages",
        )
    }}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_dev_raises_wo_username() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_dev_raises_wo_username",
            _get_s3_dependency_dir(
                "dev", "s3://bucket/", null, mock_raise_compiler_error
            ),
            "USER env var must be set when target is 'dev'",
        )
    }}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_not_dev_wont_raise_wo_username() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_not_dev_wont_raise_wo_username",
            _get_s3_dependency_dir(
                "ci", "s3://bucket/", null, exceptions.raise_compiler_error
            ),
            "s3://bucket/packages",
        )
    }}
{% endmacro %}

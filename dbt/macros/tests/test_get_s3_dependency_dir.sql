{% macro test_get_s3_dependency_dir() %}
    {% do test_get_s3_dependency_dir_target_dev() %}
    {% do test_get_s3_dependency_dir_target_ci() %}
    {% do test_get_s3_dependency_dir_target_dev_raises_wo_username() %}
    {% do test_get_s3_dependency_dir_target_dev_raises_w_space_username() %}
    {% do test_get_s3_dependency_dir_target_ci_raises_wo_head_ref() %}
    {% do test_get_s3_dependency_dir_target_ci_wont_raise_w_space_head_ref() %}
    {% do test_get_s3_dependency_dir_target_prod_wont_raise_wo_env_vars() %}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_dev() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_dev",
            _get_s3_dependency_dir(
                {"name": "dev", "s3_data_dir": "s3://bucket/"},
                mock_env_var,
                exceptions.raise_compiler_error,
            ),
            "s3://bucket/packages/test-user",
        )
    }}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_ci() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_ci",
            _get_s3_dependency_dir(
                {"name": "ci", "s3_data_dir": "s3://bucket/"},
                mock_env_var,
                exceptions.raise_compiler_error,
            ),
            "s3://bucket/packages/testuser_feature_branch_1",
        )
    }}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_dev_raises_wo_username() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_dev_raises_wo_username",
            _get_s3_dependency_dir(
                {"name": "dev", "s3_data_dir": "s3://bucket/"},
                mock_no_env_var,
                mock_raise_compiler_error,
            ),
            "USER env var must be set when target is 'dev'",
        )
    }}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_dev_raises_w_space_username() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_dev_raises_w_space_username",
            _get_s3_dependency_dir(
                {"name": "dev", "s3_data_dir": "s3://bucket/"},
                mock_space_env_var,
                mock_raise_compiler_error,
            ),
            "USER env var must be set when target is 'dev'",
        )
    }}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_ci_raises_wo_head_ref() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_ci_raises_wo_head_ref",
            _get_s3_dependency_dir(
                {"name": "ci", "s3_data_dir": "s3://bucket/"},
                mock_no_env_var,
                mock_raise_compiler_error,
            ),
            "HEAD_REF env var must be set when target is 'ci'",
        )
    }}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_ci_wont_raise_w_space_head_ref() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_ci_wont_raise_w_space_head_ref",
            _get_s3_dependency_dir(
                {"name": "ci", "s3_data_dir": "s3://bucket/"},
                mock_space_env_var,
                exceptions.raise_compiler_error,
            ),
            "s3://bucket/packages/_",
        )
    }}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_prod_wont_raise_wo_env_vars() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_prod_wont_raise_wo_env_vars",
            _get_s3_dependency_dir(
                {"name": "prod", "s3_data_dir": "s3://bucket/"},
                env_var,
                exceptions.raise_compiler_error,
            ),
            "s3://bucket/packages",
        )
    }}
{% endmacro %}

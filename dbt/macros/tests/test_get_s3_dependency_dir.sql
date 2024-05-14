{% macro test_get_s3_dependency_dir() %}
    {% do test_get_s3_dependency_dir_target_dev() %}
    {% do test_get_s3_dependency_dir_target_ci() %}
    {% do test_get_s3_dependency_dir_target_dev_raises_wo_username() %}
    {% do test_get_s3_dependency_dir_target_dev_raises_w_space_username() %}
    {% do test_get_s3_dependency_dir_target_ci_raises_wo_head_ref() %}
    {% do test_get_s3_dependency_dir_target_ci_wont_raise_w_space_head_ref() %}
    {% do test_get_s3_dependency_dir_target_prod_wont_raise_wo_env_vars() %}
    {% do test_get_s3_dependency_dir_target_unknown_raises() %}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_dev() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_dev",
            _get_s3_dependency_dir(
                "dev",
                mock_env_var,
                exceptions.raise_compiler_error,
            ),
            var("s3_dependency_dir_dev") ~ "/test-user",
        )
    }}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_ci() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_ci",
            _get_s3_dependency_dir(
                "ci",
                mock_env_var,
                exceptions.raise_compiler_error,
            ),
            var("s3_dependency_dir_ci") ~ "/testuser_feature_branch_1",
        )
    }}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_dev_raises_wo_username() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_dev_raises_wo_username",
            _get_s3_dependency_dir(
                "dev",
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
                "dev",
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
                "ci",
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
                "ci",
                mock_space_env_var,
                exceptions.raise_compiler_error,
            ),
            var("s3_dependency_dir_ci") ~ "/_",
        )
    }}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_prod_wont_raise_wo_env_vars() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_prod_wont_raise_wo_env_vars",
            _get_s3_dependency_dir(
                "prod",
                env_var,
                exceptions.raise_compiler_error,
            ),
            var("s3_dependency_dir_prod"),
        )
    }}
{% endmacro %}

{% macro test_get_s3_dependency_dir_target_unknown_raises() %}
    {{
        assert_equals(
            "test_get_s3_dependency_dir_target_unknown_raises",
            _get_s3_dependency_dir(
                "unknown",
                env_var,
                mock_raise_compiler_error,
            ),
            "target 'unknown' must be one of 'dev', 'ci', or 'prod'",
        )
    }}
{% endmacro %}

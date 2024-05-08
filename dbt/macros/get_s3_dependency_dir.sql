-- Get the S3 location that is used to store Python model dependencies
-- for the current target
{% macro get_s3_dependency_dir() %}
    {{
        return(
            _get_s3_dependency_dir(
                target.name,
                target.s3_data_dir,
                env_var("USER"),
                exceptions.raise_compiler_error,
            )
        )
    }}
{% endmacro %}

{% macro _get_s3_dependency_dir(target_name, s3_data_dir, username, raise_error_func) %}
    {% set dir_suffix = "" %}
    {% if target_name == "dev" %}
        {% if not username %}
            {{
                return(
                    raise_error_func("USER env var must be set when target is 'dev'")
                )
            }}
        {% endif %}
        {% set dir_suffix = "/" ~ username %}
    {% endif %}
    {% set s3_dependency_dir = s3_data_dir ~ "packages" ~ dir_suffix %}
    {{ return(s3_dependency_dir) }}
{% endmacro %}

-- Simple wrapper around get_s3_dependency_dir that prints the value for
-- use in external scripts
{% macro print_s3_dependency_dir() %}
    {{ print(get_s3_dependency_dir()) }}
{% endmacro %}

-- Get the S3 location that is used to store Python model dependencies
-- for the current target
{% macro get_s3_dependency_dir() %}
    {{
        return(
            _get_s3_dependency_dir(
                target,
                env_var,
                exceptions.raise_compiler_error,
            )
        )
    }}
{% endmacro %}

{% macro _get_s3_dependency_dir(target, env_var_func, raise_error_func) %}
    {% set dir_suffix = "" %}
    {% if target.name == "dev" %}
        {% set username = env_var_func("USER") | trim %}
        {% if username is none or username == "" %}
            {{
                return(
                    raise_error_func("USER env var must be set when target is 'dev'")
                )
            }}
        {% endif %}
        {% set dir_suffix = "/" ~ username %}
    {% elif target.name == "ci" %}
        {% set head_ref = slugify(env_var_func("HEAD_REF")) %}
        {% if head_ref is none or head_ref == "" %}
            {{
                return(
                    raise_error_func(
                        "HEAD_REF env var must be set when target is 'ci'"
                    )
                )
            }}
        {% endif %}
        {% set dir_suffix = "/" ~ head_ref %}
    {% endif %}
    {% set s3_dependency_dir = target.s3_data_dir ~ "packages" ~ dir_suffix %}
    {{ return(s3_dependency_dir) }}
{% endmacro %}

-- Simple wrapper around get_s3_dependency_dir that prints the value for
-- use in external scripts
{% macro print_s3_dependency_dir() %}
    {{ print(get_s3_dependency_dir()) }}
{% endmacro %}

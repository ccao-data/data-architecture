-- dbt macro unit testing inspired by this blog post:
-- https://docs.getdbt.com/blog/unit-testing-dbt-packages
{% macro test_all() %}
    {% do test_slugify() %}
    {% do test_generate_schema_name() %}
    {% do test_generate_alias_name() %}
{% endmacro %}

-- dbt macro unit testing inspired by this blog post:
-- https://docs.getdbt.com/blog/unit-testing-dbt-packages
{% macro test_all() %}
    {% do test_slugify() %}
    {% do test_generate_schema_name() %}
    {% do test_generate_alias_name() %}
    {% do test_format_additional_select_columns() %}
    {% do test_pre_stage_filters() %}
    {% do test_insert_hyphens() %}
{% endmacro %}

{% macro test_generate_alias_name() %}
    {% do test_generate_alias_name_handles_custom_alias_name() %}
    {% do test_generate_alias_name_handles_no_alias() %}
    {% do test_generate_alias_name_handles_leading_period() %}
{% endmacro %}

{% macro test_generate_alias_name_handles_custom_alias_name() %}
    {% do assert_equals(
        "test_generate_alias_name_handles_custom_alias_name",
        generate_alias_name("use_this_one", {"name": "not_this_one"}),
        "use_this_one",
    ) %}
{% endmacro %}

{% macro test_generate_alias_name_handles_no_alias() %}
    {% do assert_equals(
        "test_generate_alias_name_handles_no_alias",
        generate_alias_name(none, {"name": "use_this_one"}),
        "use_this_one",
    ) %}
{% endmacro %}

{% macro test_generate_alias_name_handles_leading_period() %}
    {% do assert_equals(
        "test_generate_alias_name_handles_leading_period",
        generate_alias_name(none, {"name": "test.use_this_one"}),
        "use_this_one",
    ) %}
{% endmacro %}

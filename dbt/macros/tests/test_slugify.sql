{% macro test_slugify() %} {% do test_kebab_slugify() %} {% endmacro %}

{% macro test_kebab_slugify() %}
    {% do test_kebab_slugify_lowercases_strings() %}
    {% do test_kebab_slugify_replaces_spaces() %}
    {% do test_kebab_slugify_replaces_slashes() %}
    {% do test_kebab_slugify_replaces_underscores() %}
    {% do test_kebab_slugify_removes_special_characters() %}
    {% do test_kebab_slugify_handles_leading_numbers() %}
{% endmacro %}

{% macro test_kebab_slugify_lowercases_strings() %}
    {{
        assert_equals(
            "test_kebab_slugify_lowercases_strings", kebab_slugify("TEST"), "test"
        )
    }}
{% endmacro %}

{% macro test_kebab_slugify_replaces_spaces() %}
    {{
        assert_equals(
            "test_kebab_slugify_replaces_spaces", kebab_slugify("t e s t"), "t-e-s-t"
        )
    }}
{% endmacro %}

{% macro test_kebab_slugify_replaces_slashes() %}
    {{
        assert_equals(
            "test_kebab_slugify_replaces_slashes", kebab_slugify("t/e/s/t"), "t-e-s-t"
        )
    }}
{% endmacro %}

{% macro test_kebab_slugify_replaces_underscores() %}
    {{
        assert_equals(
            "test_kebab_slugify_replaces_underscores",
            kebab_slugify("t_e_s_t"),
            "t-e-s-t",
        )
    }}
{% endmacro %}

{% macro test_kebab_slugify_removes_special_characters() %}
    {{
        assert_equals(
            "test_kebab_slugify_removes_special_characters",
            kebab_slugify("t!@#e$%^s&*()t"),
            "test",
        )
    }}
{% endmacro %}

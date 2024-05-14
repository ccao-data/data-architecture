{% macro test_slugify() %}
    {% do test_slugify_lowercases_strings() %}
    {% do test_slugify_replaces_spaces() %}
    {% do test_slugify_replaces_slashes() %}
    {% do test_slugify_replaces_hyphens() %}
    {% do test_slugify_removes_special_characters() %}
{% endmacro %}

{% macro test_slugify_lowercases_strings() %}
    {{ assert_equals("test_slugify_lowercases_strings", slugify("TEST"), "test") }}
{% endmacro %}

{% macro test_slugify_replaces_spaces() %}
    {{ assert_equals("test_slugify_replaces_spaces", slugify("t e s t"), "t_e_s_t") }}
{% endmacro %}

{% macro test_slugify_replaces_slashes() %}
    {{ assert_equals("test_slugify_replaces_slashes", slugify("t/e/s/t"), "t_e_s_t") }}
{% endmacro %}

{% macro test_slugify_replaces_hyphens() %}
    {{
        assert_equals(
            "test_slugify_replaces_hyphens",
            slugify("t-e-s-t"),
            "t_e_s_t",
        )
    }}
{% endmacro %}

{% macro test_slugify_removes_special_characters() %}
    {{
        assert_equals(
            "test_slugify_removes_special_characters",
            slugify("t!@#e$%^s&*()t"),
            "test",
        )
    }}
{% endmacro %}

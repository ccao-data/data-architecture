{% macro test_slugify() %}
    {% do test_slugify_lowercases_strings() %}
    {% do test_slugify_replaces_spaces() %}
    {% do test_slugify_replaces_slashes() %}
    {% do test_slugify_replaces_underscores() %}
    {% do test_slugify_removes_special_characters() %}
{% endmacro %}

{% macro test_slugify_lowercases_strings() %}
    {{ assert_equals("test_slugify_lowercases_strings", slugify("TEST"), "test") }}
{% endmacro %}

{% macro test_slugify_replaces_spaces() %}
    {{ assert_equals("test_slugify_replaces_spaces", slugify("t e s t"), "t-e-s-t") }}
{% endmacro %}

{% macro test_slugify_replaces_slashes() %}
    {{ assert_equals("test_slugify_replaces_slashes", slugify("t/e/s/t"), "t-e-s-t") }}
{% endmacro %}

{% macro test_slugify_replaces_underscores() %}
    {{
        assert_equals(
            "test_slugify_replaces_underscores",
            slugify("t_e_s_t"),
            "t-e-s-t",
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

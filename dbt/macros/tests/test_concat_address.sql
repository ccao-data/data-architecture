{% macro test_concat_address() %}
    {% do test_concat_address_replaces_control_characters() %}
    {% do test_concat_address_replaces_multiple_spaces() %}
    {% do test_concat_address_replaces_spaces_and_control_characters_with_null() %}
{% endmacro %}

{% macro test_concat_address_replaces_control_characters() %}
    {{
        assert_equals(
            "test_concat_address_replaces_control_characters",
            concat_address(["'foo'", "'bar'", "chr(15)"]),
            "foo bar",
        )
    }}
{% endmacro %}

{% macro test_concat_address_replaces_multiple_spaces() %}
    {{
        assert_equals(
            "test_concat_address_replaces_multiple_spaces",
            concat_address(["'foo '", "' bar'"]),
            "foo bar",
        )
    }}
{% endmacro %}

{% macro test_concat_address_replaces_spaces_and_control_characters_with_null() %}
    {{
        assert_equals(
            "test_concat_address_replaces_spaces_and_control_characters_with_null",
            concat_address(["chr(15)", "'   '", "chr(15)"]),
            null,
        )
    }}
{% endmacro %}
